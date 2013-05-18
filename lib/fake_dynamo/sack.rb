module FakeDynamo
  class Sack
    attr_accessor :size, :item, :max_size

    def initialize(item, max_size = 1 * 1024 * 1024) # 1 mb
      @size = 0
      @item = item
      @max_size = max_size
    end

    def has_space?
      @item.to_json.bytesize < max_size
    end
  end
end
