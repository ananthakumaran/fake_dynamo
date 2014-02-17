require 'fileutils'
require 'tempfile'

module FakeDynamo
  class Storage

    attr_accessor :compacted, :loaded, :db_path

    class << self
      def instance
        @storage ||= Storage.new
      end
    end

    def log
      Logger.log
    end

    def write_commands
      %w[CreateTable DeleteItem DeleteTable PutItem UpdateItem UpdateTable BatchWriteItem]
    end

    def write_command?(command)
      write_commands.include?(command)
    end

    def init_db(path)
      @db_path = path

      return if File.exists?(db_path) && File.writable?(db_path)

      FileUtils.mkdir_p(File.dirname(db_path))
      FileUtils.touch(db_path)
    rescue Errno::EACCES
      puts "Cannot create or access db file at #{db_path}"
      puts "Make sure you have write access to #{db_path}"
      exit(1)
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
        size = Integer(file.readline.chomp) - "\n".bytesize
        data = file.read(size); file.readline
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
      @aof.close if @aof
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
