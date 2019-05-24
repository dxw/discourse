require File.expand_path(File.dirname(__FILE__) + "/base.rb")

class ImportScripts::HigherLogic < ImportScripts::Base

  HL_ONS_HOST            ||= ENV['HL_ONS_HOST'] || "localhost"
  HL_ONS_DB              ||= ENV['HL_ONS_DB']
  HL_ONS_USER            ||= ENV['HL_ONS_USER']
  HL_ONS_PW              ||= ENV['HL_ONS_PW']
  HL_ONS_PREFIX          ||= ENV['HL_ONS_PREFIX'] || "dbo."

  BATCH_SIZE             ||= 1000
  HL_ONS_ATTACHMENTS_DIR ||= ENV.fetch('HL_ONS_ATTACHMENTS_DIR')

  LIBRARY_TAG            ||= 'library'
  ANNOUNCEMENT_TAG       ||= 'announcement'
  BLOG_TAG               ||= 'blog'

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
    SiteSetting.tagging_enabled = true

    import_groups
    import_users
    import_categories
    # import_category_group_permissions
    import_topics_and_posts
    # import_private_messages
    import_attachments
    import_announcements
    import_blogs
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
  # They have an almost 1:1 mapping: LEFT OUTER JOIN captures Communities without a Discussion
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
             Community.CommunityKey, CommunityName,
             Description, Community.CreatedOn, Community.CreatedByContactKey,
             p1.PermissionName as ViewPermissionName
      FROM #{HL_ONS_PREFIX}Community
      LEFT OUTER JOIN #{HL_ONS_PREFIX}Discussion
      ON Discussion.DiscussionKey = Community.DiscussionKey
      JOIN Permission as p1
      ON p1.PermissionKey = Community.ViewPermissionKey
      JOIN Permission as p2
      ON p2.PermissionKey = Community.JoinPermissionKey
      ORDER BY Community.CommunityKey
    SQL
    )

    create_categories(categories) do |c|
      category_name = c['DiscussionName'].to_s.strip.presence || c['CommunityName'].to_s
      category_id = c['DiscussionKey'].presence || c['CommunityKey']

      category = {
        id: category_id,
        name: category_name,
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
  # If the parent just cannot be found, we import the post as its own topic
  # Oddities: Discourse is allegedly flat, but it successfully detected nested replies within the largest thread I could find"
  # ORIGINAL thread has 79 messages:
  #   http://www.statsusernet.org.uk/communities/community-home/digestviewer/viewthread?GroupId=85&MID=6647&CommunityKey=3fb113ec-7c7f-424c-aad9-ae72f0a40f65&tab=digestviewer&ReturnUrl=%2fcommunities%2fcommunity-home%2fdigestviewer%3fcommunitykey%3d3fb113ec-7c7f-424c-aad9-ae72f0a40f65%26tab%3ddigestviewer
  # IMPORTED has 78, one gets skipped for unknown reasons:
  #   Find it in the category 'RPICPI User Group', topic 'National Statistician’s statement on the future of consumer price indices'
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

        post = {
          id: p["MessageID"],
          user_id: find_user_id(p["ContactKey"]),
          raw: format_body(p["Body"]),
          created_at: p["CreatedOn"],
        }

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
            # Could not find parent, so we will import it as its own topic,
            # by assigning it a category and title similar to the Type == New branch above
            post[:category] = category_id_from_imported_category_id(p["DiscussionKey"])
            post[:title] = CGI.unescapeHTML(p["Subject"])
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

  def find_user_id(contact_key)
    user_id_from_imported_user_id(contact_key) ||
    find_user_by_import_id(contact_key).try(:id) ||
    -1
  end

  def format_body(body)
    if body.present?
      body.gsub(/\<pre\>\<code(=[a-z]*)?\>(.*?)\<\/code\>\<\/pre\>/im) { "```\n#{@he.decode($2)}\n```" }
    else
      body
    end
  end

  def import_attachments
    import_library_entries
    import_library_entry_files
    import_item_comments_for_library_entries
  end

  def import_library_entries
    puts "", "Importing posts from LibraryEntry..."

    posts = @client.execute(<<-SQL
      SELECT LibraryEntry.DocumentKey,
             LibraryEntry.CreatedOn,
             LibraryEntry.EntryDescription,
             LibraryEntry.EntryTitle,
             LibraryEntry.ContactKey,
             Community.CommunityKey,
             Community.DiscussionKey
        FROM #{HL_ONS_PREFIX}LibraryEntry
        JOIN #{HL_ONS_PREFIX}Community
          ON LibraryEntry.LibraryKey = Community.LibraryKey
    SQL
    ).to_a

    create_posts(posts) do |p|
      # Attempt to find the category based on Community, if there was no Discussion in the first place
      original_discussion_key = p["DiscussionKey"].to_s.strip.presence
      original_community_key = p["CommunityKey"]
      category_id = category_id_from_imported_category_id(original_discussion_key || original_community_key)

      {
        id: p["DocumentKey"],
        user_id: find_user_id(p["ContactKey"]),
        raw: format_body(p["EntryDescription"]),
        created_at: p["CreatedOn"],
        category: category_id,
        title: CGI.unescapeHTML(p["EntryTitle"]),
        tags: [LIBRARY_TAG],
      }
    end
  end

  # Each LibraryEntryFile represents a single file. If `OriginalFileName` is
  # NULL we can construct the filename using `VersionName` and `FileExtension`.
  # If it's NOT NULL it means (I think!) it was renamed during upload due to a
  # name clash. In that case we might be able to use `OriginalFileName`
  # directly (without adding the extension).
  def import_library_entry_files
    puts "", "Importing files from LibraryEntryFile..."

    attachments = @client.execute(<<-SQL
      SELECT LibraryEntryFile.DocumentKey,
             LibraryEntryFile.VersionName,
             LibraryEntryFile.FileExtension,
             LibraryEntryFile.OriginalFileName,
             Library.LibraryName
        FROM #{HL_ONS_PREFIX}LibraryEntryFile
        JOIN #{HL_ONS_PREFIX}LibraryEntry
          ON LibraryEntryFile.DocumentKey = LibraryEntry.DocumentKey
        JOIN #{HL_ONS_PREFIX}Library
          ON LibraryEntry.LibraryKey = Library.LibraryKey
    SQL
    ).to_a

    total_attachments = attachments.count

    attachments.each.with_index do |a, i|
      print_status(i, total_attachments, get_start_time("import_library_entry_files"))

      path = find_file(a)

      if path
        if post = find_post_by_import_id(a["DocumentKey"])
          filename = File.basename(path)
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

  def find_file(a)
    path = File.join(HL_ONS_ATTACHMENTS_DIR, a["LibraryName"], "#{a["VersionName"]}.#{a["FileExtension"]}")
    return path if File.exists?(path)

    if a["OriginalFileName"].present?
      path = File.join(HL_ONS_ATTACHMENTS_DIR, a["LibraryName"], a["OriginalFileName"])
      return path if File.exists?(path)

      path = File.join(HL_ONS_ATTACHMENTS_DIR, a["LibraryName"], a["OriginalFileName"].split("\\").last)
      return path if File.exists?(path)

      path = File.join(HL_ONS_ATTACHMENTS_DIR, a["OriginalFileName"].split("\\").last)
      return path if File.exists?(path)
    end

    puts "Couldn't find file #{a["VersionName"]} from #{a["LibraryName"]}, last path checked: #{path}"
    nil
  end

  def import_item_comments_for_library_entries
    puts "", "Importing replies to attachments from ItemComment..."

    posts = @client.execute(<<-SQL
      SELECT ItemComment.ItemKey,
             ItemComment.Comment,
             ItemComment.ContactKey,
             ItemComment.CreatedOn
        FROM #{HL_ONS_PREFIX}ItemComment
        JOIN #{HL_ONS_PREFIX}LibraryEntry
          ON ItemComment.ItemKey = LibraryEntry.DocumentKey
       WHERE ItemType = 'LibraryEntry'
    SQL
    ).to_a

    create_posts(posts) do |p|
      parent = find_post_by_import_id(p["ItemKey"])

      # if we can't find the parent, it's for a post we didn't import
      if parent.nil?
        puts "Can't find parent post for ItemComment #{p["ItemKey"]}; skipping"
        return nil
      end

      {
        # there's no unique ID for this; since we're not going to need to
        # reference it again, we use `nil`
        id: nil,
        user_id: find_user_id(p["ContactKey"]),
        raw: format_body(p["Comment"]),
        created_at: p["CreatedOn"],
        topic_id: parent.topic_id,
      }
    end
  end

  def import_announcements
    puts "", "Importing posts from Announcement..."

    posts = @client.execute(<<-SQL
      SELECT Announcement.AnnouncementKey,
             Announcement.CreatedOn,
             Announcement.AnnouncementTitle,
             Announcement.AnnouncementText,
             Announcement.CreatedByContactKey,
             Announcement.LinkUrl,
             Announcement.LinkText,
             Community.DiscussionKey
        FROM #{HL_ONS_PREFIX}Announcement
   LEFT JOIN #{HL_ONS_PREFIX}Community
          ON Announcement.CommunityKey = Community.CommunityKey
    SQL
    ).to_a

    create_posts(posts) do |p|
      body = [
        p["AnnouncementText"],
        p["LinkUrl"].present? ? "[#{p["LinkText"]}](#{p["LinkUrl"]})" : ""
      ].reject(&:blank?).join("\n\n")

      {
        id: p["AnnouncementKey"],
        user_id: find_user_id(p["CreatedByContactKey"]),
        raw: format_body(body),
        created_at: p["CreatedOn"],
        category: category_id_from_imported_category_id(p["DiscussionKey"]),
        title: CGI.unescapeHTML(p["AnnouncementTitle"]),
        tags: [ANNOUNCEMENT_TAG],
      }
    end
  end

  def import_blogs
    import_posts_from_blog
    import_item_comments_for_blogs
  end

  def import_posts_from_blog
    puts "", "Importing posts from Blog..."

    posts = @client.execute(<<-SQL
      SELECT Blog.BlogKey,
             Blog.PublishedOn,
             Blog.Title,
             Blog.Description,
             Blog.CreatedByContactKey,
             Community.DiscussionKey
        FROM #{HL_ONS_PREFIX}Blog
   LEFT JOIN #{HL_ONS_PREFIX}Community
          ON Blog.CommunityKey = Community.CommunityKey
    SQL
    ).to_a

    create_posts(posts) do |p|
      {
        id: p["BlogKey"],
        user_id: find_user_id(p["CreatedByContactKey"]),
        raw: format_body(p["Description"]),
        created_at: p["PublishedOn"],
        category: category_id_from_imported_category_id(p["DiscussionKey"]),
        title: CGI.unescapeHTML(p["Title"]),
        tags: [BLOG_TAG],
      }
    end
  end

  def import_item_comments_for_blogs
    puts "", "Importing replies to blogs from ItemComment..."

    posts = @client.execute(<<-SQL
      SELECT ItemComment.ItemKey,
             ItemComment.Comment,
             ItemComment.ContactKey,
             ItemComment.CreatedOn
        FROM #{HL_ONS_PREFIX}ItemComment
        JOIN #{HL_ONS_PREFIX}Blog
          ON ItemComment.ItemKey = Blog.BlogKey
       WHERE ItemType = 'Blog'
    SQL
    ).to_a

    create_posts(posts) do |p|
      parent = find_post_by_import_id(p["ItemKey"])

      # if we can't find the parent, it's for a post we didn't import
      if parent.nil?
        puts "Can't find parent post for ItemComment #{p["ItemKey"]}; skipping"
        return nil
      end

      {
        # there's no unique ID for this; since we're not going to need to
        # reference it again, we use `nil`
        id: nil,
        user_id: find_user_id(p["ContactKey"]),
        raw: format_body(p["Comment"]),
        created_at: p["CreatedOn"],
        topic_id: parent.topic_id,
      }
    end
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
