$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'rubygems'
require 'bundler/setup'
require 'rspec'
require 'logger'

require 'rails'
require 'active_record'
require 'action_mailer'

require 'delayed_job'
require 'delayed/backend/shared_spec'

Delayed::Worker.logger = Logger.new('/tmp/dj.log')
ENV['RAILS_ENV'] = 'test'

config = YAML.load(File.read('spec/database.yml'))
ActiveRecord::Base.configurations = {'test' => config['mysql']}
ActiveRecord::Base.establish_connection
ActiveRecord::Base.logger = Delayed::Worker.logger
ActiveRecord::Migration.verbose = false

ActiveRecord::Schema.define do
  create_table :delayed_jobs, :force => true do |table|
      table.integer  :priority, :default => 0      # Allows some jobs to jump to the front of the queue
      table.integer  :attempts, :default => 0      # Provides for retries, but still fail eventually.
      table.text     :handler                      # String name of the constant. Must respond to .call
      table.text     :arguments_json               # JSON-encoded string of the payload
      table.text     :last_error                   # reason for last failure (See Note below)
      table.datetime :run_at                       # When to run. Could be Time.zone.now for immediately, or sometime in the future.
      table.datetime :locked_at                    # Set when a client is working on this object
      table.datetime :failed_at                    # Set when all retries have failed (actually, by default, the record is deleted instead)
      table.string   :locked_by                    # Who is working on this object (if locked)
      table.timestamps
  end

  add_index :delayed_jobs, [:priority, :run_at], :name => 'delayed_jobs_priority'

  create_table :stories, :force => true do |table|
    table.string :text
  end
end

# Purely useful for test cases...
class Story < ActiveRecord::Base
  def tell; text; end
  def whatever(n, _); tell*n; end

  handle_asynchronously :whatever
end

Delayed::Worker.backend = :active_record

# Add this directory so the ActiveSupport autoloading works
ActiveSupport::Dependencies.autoload_paths << File.dirname(__FILE__)

# Add this to simulate Railtie initializer being executed
ActionMailer::Base.send(:extend, Delayed::DelayMail)
