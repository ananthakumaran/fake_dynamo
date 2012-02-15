module FakeDynamo
  class Value
    attr_accessor :value, :type

    def initialize(value, type)
      @value, @type = value, type
    end
  end
end
