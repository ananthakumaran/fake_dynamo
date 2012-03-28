module FakeDynamo
  module Filter
    include Validation

    def comparison_filter(value_list, size, target_attribute, fail_on_type_mismatch, supported_types, comparator)
      return false if target_attribute.nil?

      validate_size(value_list, size)

      if fail_on_type_mismatch
        value_list.each do |value|
          validate_type(value, target_attribute)
        end
      end

      value_attribute_list = value_list.map do |value|
        value_attribute = Attribute.from_hash(target_attribute.name, value)
        validate_supported_types(value_attribute, supported_types)
        value_attribute
      end

      value_attribute_list.each do |value_attribute|
        return false if target_attribute.type != value_attribute.type
      end

      if target_attribute.type == 'N'
        comparator.call(target_attribute.value.to_f, *value_attribute_list.map(&:value).map(&:to_f))
      else
        comparator.call(target_attribute.value, *value_attribute_list.map(&:value))
      end
    end

    def validate_supported_types(value_attribute, supported_types)
      unless supported_types.include? value_attribute.type
        raise ValidationException, "The attempted filter operation is not supported for the provided type"
      end
    end

    def validate_size(value_list, size)
      if (size.kind_of? Range and (not (size.include? value_list.size))) or
          (size.kind_of? Integer and value_list.size != size)
        raise ValidationException, "The attempted filter operation is not supported for the provided filter argument count"
      end
    end

    def self.def_filter(name, size, supported_types, &comparator)
      define_method "#{name}_filter" do |value_list, target_attribute, fail_on_type_mismatch|
        comparison_filter(value_list, size, target_attribute, fail_on_type_mismatch, supported_types, comparator)
      end
    end

    def_filter(:eq, 1, ['N', 'S'], &:==)
    def_filter(:le, 1, ['N', 'S'], &:<=)
    def_filter(:lt, 1, ['N', 'S'], &:<)
    def_filter(:ge, 1, ['N', 'S'], &:>=)
    def_filter(:gt, 1, ['N', 'S'], &:>)
    def_filter(:begins_with, 1, ['S'], &:start_with?)
    def_filter(:between, 2, ['N', 'S'], &:between?)
    def_filter(:ne, 1, ['N', 'S'], &:!=)

    def not_null_filter(value_list, target_attribute,  fail_on_type_mismatch)
      not target_attribute.nil?
    end

    def null_filter(value_list, target_attribute, fail_on_type_mismatch)
      target_attribute.nil?
    end

    def contains_filter(value_list, target_attribute, fail_on_type_mismatch)
      return false if target_attribute.nil?

      validate_size(value_list, 1)
      value_attribute = Attribute.from_hash(target_attribute.name, value_list.first)
      validate_supported_types(value_attribute, ['N', 'S'])

      if ((value_attribute.type == 'S' and
           (target_attribute.type == 'S' or target_attribute.type == 'SS')) or
          (value_attribute.type == 'N' and target_attribute.type == 'NS'))
        target_attribute.value.include?(value_attribute.value)
      end
    end

    def not_contains_filter(value_list, target_attribute, fail_on_type_mismatch)
      return false if target_attribute.nil?

      validate_size(value_list, 1)
      value_attribute = Attribute.from_hash(target_attribute.name, value_list.first)
      validate_supported_types(value_attribute, ['N', 'S'])

      if ((value_attribute.type == 'S' and
           (target_attribute.type == 'S' or target_attribute.type == 'SS')) or
          (value_attribute.type == 'N' and target_attribute.type == 'NS'))
        !target_attribute.value.include?(value_attribute.value)
      end
    end

    INF = 1.0/0.0

    def in_filter(value_list, target_attribute, fail_on_type_mismatch)
      return false if target_attribute.nil?

      validate_size(value_list, (1..INF))

      value_attribute_list = value_list.map do |value|
        value_attribute = Attribute.from_hash(target_attribute.name, value)
        validate_supported_types(value_attribute, ['N', 'S'])
        value_attribute
      end

      value_attribute_list.each do |value_attribute|
        return true if value_attribute == target_attribute
      end

      false
    end
  end
end
