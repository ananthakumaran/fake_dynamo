require 'fileutils'
require 'tempfile'

module FakeDynamo
  class Storage

    attr_accessor :compacted, :loaded

    class << self
      attr_accessor :db_path

      def instance
        @storage ||= Storage.new
      end
    end

    def log
      Logger.log
    end

    def initialize
      init_db
    end

    def write_commands
      %w[CreateTable DeleteItem DeleteTable PutItem UpdateItem UpdateTable BatchWriteItem]
    end

    def write_command?(command)
      write_commands.include?(command)
    end

    def db_path
      self.class.db_path
    end

    def init_db
      return if File.exists? db_path
      FileUtils.mkdir_p(File.dirname(db_path))
      FileUtils.touch(db_path)
    end

    def delete_db
      return unless File.exists? db_path
      FileUtils.rm(db_path)
    end

    def reset
      log.warn "resetting database ..."
      @aof.close if @aof
      @aof = nil
      delete_db
    end

    def db
      DB.instance
    end

    def db_aof
      @aof ||= File.new(db_path, 'a')
    end

    def shutdown
      log.warn "shutting down fake_dynamo ..."
      @aof.close if @aof
    end

    def persist(operation, data)
      return unless write_command?(operation)
      db_aof.puts(operation)
      data = data.to_json
      db_aof.puts(data.bytesize + "\n".bytesize)
      db_aof.puts(data)
      db_aof.flush
    end

    def load_aof
      return if @loaded
      file = File.new(db_path, 'r')
      log.warn "Loading fake_dynamo data ..."
      loop do
        operation = file.readline.chomp
        size = Integer(file.readline.chomp)
        data = file.read(size)
        db.process(operation, JSON.parse(data))
      end
    rescue EOFError
      file.close
      compact_if_necessary
      @loaded = true
    end

    def compact_threshold
      100 * 1024 * 1024 # 100mb
    end

    def compact_if_necessary
      return unless File.exists? db_path
      if File.stat(db_path).size > compact_threshold
        compact!
      end
    end

    def compact!
      return if @compacted
      @aof = Tempfile.new('compact')
      log.warn "Compacting db ..."
      db.tables.each do |_, table|
        persist('CreateTable', table.create_table_data)
        table.items.each do |_, item|
          persist('PutItem', table.put_item_data(item))
        end
      end
      @aof.close
      FileUtils.mv(@aof.path, db_path)
      @aof = nil
      @compacted = true
    end
  end
end
