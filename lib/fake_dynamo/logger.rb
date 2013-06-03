module FakeDynamo
  module Logger
    class << self
      attr_accessor :log

      def setup(level)
        logger = ::Logger.new(STDOUT)
        logger.level = [:debug, :info, :warn, :error, :fatal].index(level)
        logger.formatter = proc do |severity, datetime, progname, msg|
          "#{msg}\n"
        end

        def logger.pp(object)
          return if level > ::Logger::INFO
          output = ''
          PP.pp(object, output)
          info(output)
        end

        @log = logger
      end
    end
  end
end
