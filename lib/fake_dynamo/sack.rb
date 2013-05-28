module FakeDynamo
  class Sack
    attr_accessor :size, :item, :max_size

    def initialize(max_size = 1 * 1024 * 1024) # 1 mb
      @size = 0
      @max_size = max_size
    end

    def add(item)
      @size += item.to_json.bytesize
    end

    def has_space?
      @size < @max_size
    end
  end
end
