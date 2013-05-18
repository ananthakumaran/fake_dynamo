module FakeDynamo
  class Item
    include Validation
    attr_accessor :key, :attributes

    class << self
      def from_data(data, key_schema)
        item = Item.new
        item.key = Key.from_schema(data, key_schema)

        item.attributes = {}
        data.each do |name, value|
          unless item.key[name]
            item.attributes[name] = Attribute.from_hash(name, value)
          end
        end
        item
      end

      def from_key(key)
        item = Item.new
        item.key = key
        item.attributes = {}
        item
      end
    end


    def [](name)
      attributes[name] or key[name]
    end

    def as_hash
      result = {}
      result.merge!(key.as_hash)
      @attributes.each do |name, attribute|
        result.merge!(attribute.as_hash)
      end
      result
    end

    def to_json(*)
      as_hash.to_json
    end

    def update(name, data)
      if key[name]
        raise ValidationException, "Cannot update attribute #{name}. This attribute is part of the key"
      end

      new_value = data['Value']
      action = data['Action'] || 'PUT'

      unless available_actions.include? action
        raise ValidationException, "Unknown action '#{action}' in AttributeUpdates.#{name}"
      end

      if (not new_value) and action != 'DELETE'
        raise ValidationException, "Only DELETE action is allowed when no attribute value is specified"
      end

      self.send(action.downcase, name, new_value)
    end

    def available_actions
      %w[ PUT ADD DELETE ]
    end

    def put(name, value)
      attributes[name] = Attribute.from_hash(name, value)
    end

    def delete(name, value)
      if not value
        attributes.delete(name)
      elsif old_attribute = attributes[name]
        validate_type(value, old_attribute)
        unless ["SS", "NS", "BS"].include? old_attribute.type
          raise ValidationException, "Action DELETE is not supported for type #{old_attribute.type}"
        end
        attribute = Attribute.from_hash(name, value)
        old_attribute.value -= attribute.value
      end
    end

    def add(name, value)
      attribute = Attribute.from_hash(name, value)

      unless ["N", "SS", "NS", "BS"].include? attribute.type
        raise ValidationException, "Action ADD is not supported for type #{attribute.type}"
      end

      if old_attribute = attributes[name]
        validate_type(value, old_attribute)
        case attribute.type
        when "N"
          new_value = (old_attribute.value.to_f + attribute.value.to_f)
          if new_value.truncate == new_value
            new_value = new_value.truncate
          end

          old_attribute.value = new_value.to_s
        else
          old_attribute.value += attribute.value
          old_attribute.value.uniq!
        end
      else
        attributes[name] = attribute
      end
    end

    def collection_metrics(data)
      if data['ReturnItemCollectionMetrics'] == 'SIZE'
        {  'ItemCollectionMetrics' =>
          { 'ItemCollectionKey' => key.primary.as_hash,
            'SizeEstimateRangeGB' => [ 0, 1 ] } }
      else
        {}
      end
    end
  end
end
