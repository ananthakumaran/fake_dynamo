module FakeDynamo
  class Storage

    class << self
      def instance
        @storage ||= Storage.new
      end
    end

    def initialize
      init_db
    end

    def write_commands
      %w[CreateTable DeleteItem DeleteTable PutItem UpdateItem UpdateTable]
    end

    def write_command?(command)
      write_commands.include?(command)
    end

    def db_path
      '/usr/local/var/fake_dynamo/db.fdb'
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

    def db
      DB.instance
    end

    def db_aof
      @aof ||= File.new(db_path, 'a')
    end

    def shutdown
      puts "shutting down fake_dynamo ..."
      @aof.close if @aof
    end

    def persist(operation, data)
      return unless write_command?(operation)
      db_aof.puts(operation)
      data = data.to_json
      db_aof.puts(data.size + 1)
      db_aof.puts(data)
    end

    def load_aof
      file = File.new(db_path, 'r')
      puts "Loading fake_dynamo data ..."
      loop do
        operation = file.readline.chomp
        size = Integer(file.readline.chomp)
        data = file.read(size)
        db.process(operation, JSON.parse(data))
      end
    rescue EOFError
      file.close
    end
  end
end
