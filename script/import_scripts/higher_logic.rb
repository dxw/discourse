require File.expand_path(File.dirname(__FILE__) + "/base.rb")

class ImportScripts::HigherLogic < ImportScripts::Base

  HL_ONS_HOST            ||= ENV['HL_ONS_HOST'] || "localhost"
  HL_ONS_DB              ||= ENV['HL_ONS_DB']
  HL_ONS_USER            ||= ENV['HL_ONS_USER']
  HL_ONS_PW              ||= ENV['HL_ONS_PW']
  HL_ONS_PREFIX          ||= ENV['HL_ONS_PREFIX'] || "dbo."

  BATCH_SIZE             ||= 1000
  # HL_ONS_ATTACHMENTS_DIR ||= ENV['HL_ONS_ATTACHMENTS_DIR'] || "/path/to/attachments"

  def initialize
    super

    @he = HTMLEntities.new

    @client = TinyTds::Client.new(
      host: HL_ONS_HOST,
      database: HL_ONS_DB,
      username: HL_ONS_USER,
      password: HL_ONS_PW,
    )
  end

  def execute
    import_groups
    import_users
    import_categories
    # import_category_group_permissions
    import_topics_and_posts
    # import_private_messages
    # import_attachments
    # create_permalinks
  end

  ### Community --> Group, but only for those Communities that have some restrictive permission, such as https://dxw.slack.com/archives/CHC2H69HP/p1556205608011800
  # through JoinPermissionKey and ViewPermissionKey
  ### SecurityGroup --> Group, but only for the custom SecGroups that have a meaning, such as https://dxw.slack.com/archives/CHC2H69HP/p1556204065011500
  def import_groups
    puts "Importing groups..."
    puts "NOT IMPLEMENTED!"
  end

  ### Contact --> User
  # Primary key: ContactKey
  # Oddities:
  # * Contact did not use any of the DisplayName or nickname columns, so we have to make up a username, which Discourse requires
  # * Discourse doesn't have forenames / surnames, so we take our best guess by concatenating the original FirstName and LastName
  # * A very small number of Contacts, and only one of them 'Enabled', don't have an email address registered, we have to skip them
  #
  # Discourse has category_groups, which could probably give them some of the functionality of the closed, invitation-only communities
  ## CommunityMember --> GroupUser to join with Group, if created at import_groups
  ## ContactSecurityGroup --> GroupUser
  def import_users
    puts "", "Importing users..."

    total_users = @client.execute(<<-SQL
      SELECT COUNT(DISTINCT(ContactKey)) AS cnt
      FROM #{HL_ONS_PREFIX}Contact
      WHERE EmailAddress IS NOT NULL
      AND UserStatus != 'Disabled'
    SQL
    ).first["cnt"]

    batches(BATCH_SIZE) do |offset|
      users = @client.execute(<<-SQL
        SELECT u.ContactKey, u.EmailAddress, u.FirstName, u.LastName, u.CreatedOn, HLAdminFlag, lastLoginDate
          FROM #{HL_ONS_PREFIX}Contact u
          JOIN (SELECT ContactKey, max(LoginDate) as lastLoginDate FROM ContactLoginDate group by ContactKey) as l
          ON u.ContactKey = l.ContactKey
          WHERE EmailAddress IS NOT NULL
          AND UserStatus != 'Disabled'
          ORDER BY u.ContactKey
          OFFSET #{offset} rows fetch next #{BATCH_SIZE} rows only
      SQL
      ).to_a

      break if users.empty?

      create_users(users, total: total_users, offset: offset) do |u|
        {
          id: u['ContactKey'],
          username: u['EmailAddress'].to_s,
          email: u['EmailAddress'].to_s.downcase,
          name: [u['FirstName'].to_s, u['LastName'].to_s].compact.join(' '),
          created_at: u['CreatedOn'],
          last_seen_at: u['lastLoginDate'],
          admin: u['HLAdminFlag']
        }
      end
    end
  end

  ### Community + Discussion --> Category
  # Primary key: DiscussionKey
  # They have an almost 1:1 mapping
  #
  # Community is the entity carrying the permissions info, both as Join and View permissions.
  #
  # A first attempt at restricting viewing permissions: if the original was anything but 'Public', restrict it (can always be opened up)
  #
  # Discourse has category_groups, which could probably give them some of the functionality of the closed, invitation-only communities.
  # It also has a set number of "autogroups", one of which is 'trust_level_0' for Authenticated only, and would give them more than the
  # binary of 'read_restricted'.
  #
  # CommunityMember also has information about subscriptions and email addresses, which they were interested in keeping,
  # and could possibly map with Discourse's category_users.
  def import_categories
    puts "", "Importing categories..."

    categories = @client.execute(<<-SQL
      SELECT Discussion.DiscussionKey, DiscussionName,
             Description, Community.CreatedOn, Community.CreatedByContactKey,
             p1.PermissionName as ViewPermissionName
      FROM #{HL_ONS_PREFIX}Discussion
      JOIN #{HL_ONS_PREFIX}Community
      ON Discussion.DiscussionKey = Community.DiscussionKey
      JOIN Permission as p1
      ON p1.PermissionKey = Community.ViewPermissionKey
      JOIN Permission as p2
      ON p2.PermissionKey = Community.JoinPermissionKey
      ORDER BY Discussion.DiscussionKey
    SQL
    )

    create_categories(categories) do |c|
      category = {
        id: c['DiscussionKey'],
        name: c['DiscussionName'],
        description: c['Description'],
        created_at: c['CreatedOn'],
        user_id: user_id_from_imported_user_id(c['CreatedByContactKey']),
        read_restricted: c['ViewPermissionName'].to_s.downcase != 'public'
      }
      category
    end
  end

  # TODO! The final piece would be to join those restricted categories with groups created at the previous steps
  # and give the join the kind of permission_type that applies
  # I don't know if 1 (:full) means permission to delete or just create topic
  # 2 (:create_post) seems to be to comment on existing topics, rather than create new topics
  # 3 is the clearest: :readonly
  def import_category_group_permissions
    # pseudocode ahoy!
    categories.each do |ic|
      category = Category.find(category_id_from_imported_category_id(ic['DiscussionKey']))
      next unless category

      case ic['ViewPermissionName']
        # we don't need to do anything if it's already 'Public', it would have been created as not read_restricted and everyone can view
      when 'Authenticated'
        CategoryGroup.create!(category: category, group: Group::AUTO_GROUPS[:trust_level_0])
      when 'MembersOnly', 'InvitationOnly'
        # TODO, probably:
        # * retrieve the group for this category and create a category_group to join the categ with the group
        # * give it a permission_type from the enum (full: 1, create_post: 2, readonly: 3)
        # CategoryGroup.create!(category: category, group: CUSTOM_GROUP_FOR_THAT_COMMUNITY)
      end
    end
  end

  ### DiscussionPost --> Post
  # Primary key: we've chosen MessageID, because it can be sorted in a more predictable order than MessageKey
  # Topics get created with the same method as posts, based on the post not having a 'topic_id' attribute
  # Topics must belong to a category, and must have a title
  # Posts must belong to a topic
  # The automagical topic grouping is the least understood (by me) part of the magic import scripts
  # The original script didn't have a way to detect the topic for a reply-to-a-reply,
  #   because the ParentMessageID points to the direct parent,
  #   but does not point to the topic.
  # I have modified the script to attempt finding the imported topic from the imported parent, and assign it to the n-th reply.
  # Oddities: Discourse is allegedly flat, but it successfully detected nested replies within the largest thread I could find"
  # ORIGINAL thread has 79 messages:
  #   http://www.statsusernet.org.uk/communities/community-home/digestviewer/viewthread?GroupId=85&MID=6647&CommunityKey=3fb113ec-7c7f-424c-aad9-ae72f0a40f65&tab=digestviewer&ReturnUrl=%2fcommunities%2fcommunity-home%2fdigestviewer%3fcommunitykey%3d3fb113ec-7c7f-424c-aad9-ae72f0a40f65%26tab%3ddigestviewer
  # IMPORTED has 78, one gets skipped for unknown reasons:
  #   Find it in the category 'RPICPI User Group', topic 'National Statistician’s statement on the future of consumer price indices'

  ### A big one we forgot about is Blog! They only seem connected by DiscussionKey and ContactKey, so not topics on their own,
  # but if Discourse doesn't have a specific blog-like entity, we might have to import them as topics...
  def import_topics_and_posts
    puts "", "Importing topics and posts..."

    total_posts = @client.execute(<<-SQL
      SELECT COUNT(*) count
        FROM #{HL_ONS_PREFIX}DiscussionPost
        JOIN #{HL_ONS_PREFIX}Discussion
        ON Discussion.DiscussionKey = DiscussionPost.DiscussionKey
    SQL
    ).first["count"]

    batches(BATCH_SIZE) do |offset|
      posts = @client.execute(<<-SQL
        SELECT MessageKey, MessageID,
               Type, MessageThreadKey,
               CreatedOn, Body, Subject, ContactKey,
               ParentMessageKey, ParentMessageID,
               DiscussionPost.DiscussionKey, Discussion.DiscussionName
          FROM #{HL_ONS_PREFIX}DiscussionPost
          JOIN #{HL_ONS_PREFIX}Discussion
          ON Discussion.DiscussionKey = DiscussionPost.DiscussionKey
          ORDER BY MessageID
          OFFSET #{offset} rows fetch next #{BATCH_SIZE} rows only
      SQL
      ).to_a

      break if posts.empty?

      create_posts(posts, total: total_posts, offset: offset) do |p|
        skip = false

        user_id = user_id_from_imported_user_id(p["ContactKey"]) ||
                  find_user_by_import_id(p["ContactKey"]).try(:id) ||
                  -1

        post = {
          id: p["MessageID"],
          user_id: user_id,
          raw: p["Body"],
          created_at: p["CreatedOn"],
        }

        discussion_name = p["DiscussionName"]

        if post[:raw].present?
          post[:raw].gsub!(/\<pre\>\<code(=[a-z]*)?\>(.*?)\<\/code\>\<\/pre\>/im) { "```\n#{@he.decode($2)}\n```" }
        end

        if p["Type"] == "New"
          post[:category] = category_id_from_imported_category_id(p["DiscussionKey"])
          post[:title] = CGI.unescapeHTML(p["Subject"])
        else
          if parent = topic_lookup_from_imported_post_id(p["ParentMessageID"])
            post[:topic_id] = parent[:topic_id]
            post[:reply_to_post_number] = parent[:post_number] if parent[:post_number] > 1
          elsif parent = find_post_by_import_id(p["ParentMessageID"])
            post[:topic_id] = parent.topic_id
            post[:reply_to_post_number] = parent.post_number if parent.post_number > 1
          else
            # We *could* instead import it as its own topic, by changing its Type to 'New',
            # and assigning it a category and title similar to the Type == New branch above (we have the DiscussionKey and a Subject)
            puts "Skipping #{p["MessageKey"]} from #{discussion_name}: #{p["Subject"]} | Parent #{p["ParentMessageKey"]} | Thread #{p["MessageThreadKey"]}"
            skip = true
          end
        end

        skip ? nil : post
      end
    end
  end

  # The ENTITYCustomField seem to get populated by ~magic.
  # The value of 'import_id' is the value of whatever we have designated as :id in the hash that is yielded to create_* methods (create_categories, create_users etc)
  # For example, for a user we have given { id: 'ContactKey' }, because that's the primary key
  # The custom fields belong to the Discourse record created from the original parameters
  def find_post_by_import_id(import_id)
    PostCustomField.where(name: 'import_id', value: import_id.to_s).first.try(:post)
  end

  # everything from here on is unmodified bbpress-specific import code
  def import_attachments
    import_attachments_from_postmeta
    import_attachments_from_bb_attachments
  end

  def import_attachments_from_postmeta
    puts "", "Importing attachments from 'postmeta'..."

    count = 0
    last_attachment_id = -1

    total_attachments = bbpress_query(<<-SQL
      SELECT COUNT(*) count
        FROM #{HL_ONS_PREFIX}postmeta pm
        JOIN #{HL_ONS_PREFIX}posts p ON p.id = pm.post_id
       WHERE pm.meta_key = '_wp_attached_file'
         AND p.post_parent > 0
    SQL
    ).first["count"]

    batches(BATCH_SIZE) do |offset|
      attachments = bbpress_query(<<-SQL
        SELECT pm.meta_id id, pm.meta_value, p.post_parent post_id
          FROM #{HL_ONS_PREFIX}postmeta pm
          JOIN #{HL_ONS_PREFIX}posts p ON p.id = pm.post_id
         WHERE pm.meta_key = '_wp_attached_file'
           AND p.post_parent > 0
           AND pm.meta_id > #{last_attachment_id}
      ORDER BY pm.meta_id
         LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if attachments.empty?
      last_attachment_id = attachments[-1]["id"].to_i

      attachments.each do |a|
        print_status(count += 1, total_attachments, get_start_time("attachments_from_postmeta"))
        path = File.join(BB_PRESS_ATTACHMENTS_DIR, a["meta_value"])
        if File.exists?(path)
          if post = Post.find_by(id: post_id_from_imported_post_id(a["post_id"]))
            filename = File.basename(a["meta_value"])
            upload = create_upload(post.user.id, path, filename)
            if upload&.persisted?
              html = html_for_upload(upload, filename)
              if !post.raw[html]
                post.raw << "\n\n" << html
                post.save!
                PostUpload.create!(post: post, upload: upload) unless PostUpload.where(post: post, upload: upload).exists?
              end
            end
          end
        end
      end
    end
  end

  def find_attachment(filename, id)
    @attachments ||= Dir[File.join(BB_PRESS_ATTACHMENTS_DIR, "vf-attachs", "**", "*.*")]
    @attachments.find { |p| p.end_with?("/#{id}.#{filename}") }
  end

  def create_permalinks
    puts "", "creating permalinks..."

    last_topic_id = -1

    batches(BATCH_SIZE) do |offset|
      topics = bbpress_query(<<-SQL
        SELECT id,
               guid
          FROM #{HL_ONS_PREFIX}posts
         WHERE post_status <> 'spam'
           AND post_type IN ('topic')
           AND id > #{last_topic_id}
      ORDER BY id
         LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if topics.empty?
      last_topic_id = topics[-1]["id"].to_i

      topics.each do |t|
        topic = topic_lookup_from_imported_post_id(t['id'])
        Permalink.create(url: URI.parse(t['guid']).path.chomp('/'), topic_id: topic[:topic_id]) rescue nil
      end
    end
  end
end

ImportScripts::HigherLogic.new.perform
