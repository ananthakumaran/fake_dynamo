module FakeDynamo
  class Projection
    extend Validation
    attr_accessor :type, :non_key_attributes

    def initialize(type, non_key_attributes)
      @type, @non_key_attributes = type, non_key_attributes
    end

    class << self
      def from_data(data)
        projection = Projection.new(data['ProjectionType'], data['NonKeyAttributes'])
        validate_projection(projection)
        projection
      end
    end

    def description
      {'ProjectionType' => type}.merge(non_key_attributes_description)
    end

    def non_key_attributes_description
      if non_key_attributes
        {'NonKeyAttributes' => @non_key_attributes}
      else
        {}
      end
    end
  end
end
