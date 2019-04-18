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
    import_users
    import_categories
    # import_topics_and_posts
    # import_private_messages
    # import_attachments
    # create_permalinks
  end

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
          last_seen_at: u['lastLoginDate']
        }
      end
    end
  end

  def import_categories
    puts "", "Importing categories..."

    categories = @client.execute(<<-SQL
      SELECT Discussion.DiscussionKey, DiscussionName, Description, Contact.ContactKey, Community.CreatedOn
      FROM #{HL_ONS_PREFIX}Discussion
      JOIN #{HL_ONS_PREFIX}Community
      ON Discussion.DiscussionKey = Community.DiscussionKey
      JOIN #{HL_ONS_PREFIX}Contact
      ON Community.CreatedByContactKey = Contact.ContactKey
      ORDER BY Discussion.DiscussionKey
    SQL
    )

    # Trying to assign the category.user_id from the Community.CreatedByContactKey is a TRAP:
    # for whatever reason (magic cessation spell?) it does not find the correspondence with the sql id of the imported user,
    # even when the user with that ContactKey has already been imported.
    # So we'll have to settle for the system user to be the creator of all these communities,
    # which is pretty certain to displease the actual creators, given how displeased they are about the move already.
    create_categories(categories) do |c|
      category = {
        id: c['DiscussionKey'],
        name: c['DiscussionName'],
        description: c['Description'],
        created_at: c['CreatedOn'],
      }
      category
    end
  end

  def import_topics
    puts "", "importing topics"

    total_topics = @client.execute(<<-SQL
      SELECT COUNT(*) count
        FROM #{HL_ONS_PREFIX}Discussion
    SQL
    ).first["count"]

    batches(BATCH_SIZE) do |offset|
      posts = @client.execute(<<-SQL
        SELECT DiscussionKey,
               DiscussionName,
               CreatedOn,
               Body,
               Subject,
               Type,
               ParentMessageKey
          FROM #{HL_ONS_PREFIX}DiscussionPost
          ORDER BY MessageKey
          OFFSET #{offset} rows fetch next #{BATCH_SIZE} rows only
      SQL
      ).to_a

      break if posts.empty?
    end
  end

  def import_posts
    puts "", "importing posts..."

    total_posts = @client.execute(<<-SQL
      SELECT COUNT(*) count
        FROM #{HL_ONS_PREFIX}DiscussionPost
    SQL
    ).first["count"]

    batches(BATCH_SIZE) do |offset|
      posts = @client.execute(<<-SQL
        SELECT MessageKey,
               ContactKey,
               CreatedOn,
               Body,
               Subject,
               Type,
               ParentMessageKey
          FROM #{HL_ONS_PREFIX}DiscussionPost
          ORDER BY MessageKey
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
          id: p["MessageKey"],
          user_id: user_id,
          raw: p["Body"],
          created_at: p["CreatedOn"],
        }

        if post[:raw].present?
          post[:raw].gsub!(/\<pre\>\<code(=[a-z]*)?\>(.*?)\<\/code\>\<\/pre\>/im) { "```\n#{@he.decode($2)}\n```" }
        end

        if p["post_type"] == "topic"
          post[:category] = category_id_from_imported_category_id(p["post_parent"])
          post[:title] = CGI.unescapeHTML(p["post_title"])
        else
          if parent = topic_lookup_from_imported_post_id(p["post_parent"])
            post[:topic_id] = parent[:topic_id]
            post[:reply_to_post_number] = parent[:post_number] if parent[:post_number] > 1
          else
            puts "Skipping #{p["id"]}: #{p["post_content"][0..40]}"
            skip = true
          end
        end

        skip ? nil : post
      end
    end
  end

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
