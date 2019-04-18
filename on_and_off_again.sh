be rake db:drop RAILS_ENV=development
be rake db:create RAILS_ENV=development
psql discourse_development < basic_setup
