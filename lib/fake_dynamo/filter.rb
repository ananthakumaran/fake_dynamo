module FakeDynamo
  module Filter

    def comparison_filter(value_list, size, attribute, attribute_name, fail_on_type_mismatch, supported_types, comparator)

      if value_list.size != size
        raise ValidationException, "The attempted filter operation is not supported for the provided filter argument count"
      end

      value = value_list.first
      if fail_on_type_mismatch
        value_list.each do |value|
          validate_type(value, attribute)
        end
      end

      value_attribute_list = value_list.map do |value|
        value_attribute = Attribute.from_hash(attribute_name, value)
        unless supported_types.include? value_attribute.type
          raise ValidationException, "The attempted filter operation is not supported for the provided type"
        end
        value_attribute
      end

      value_attribute_list.each do |value_attribute|
        return false if attribute.type != value_attribute.type
      end

      if attribute.type == 'N'
        comparator.call(attribute.value.to_i, *value_attribute_list.map(&:value).map(&:to_i))
      else
        comparator.call(attribute.value, *value_attribute_list.map(&:value))
      end
    end

    def self.def_filter(name, size, supported_types, &comparator)
      define_method "#{name}_filter" do |value_list, attribute, attribute_name, fail_on_type_mismatch|
        comparison_filter(value_list, size, attribute, attribute_name, fail_on_type_mismatch, supported_types, comparator)
      end
    end

    def_filter(:eq, 1, ['N', 'S'], &:==)
    def_filter(:le, 1, ['N', 'S'], &:<=)
    def_filter(:lt, 1, ['N', 'S'], &:<)
    def_filter(:ge, 1, ['N', 'S'], &:>=)
    def_filter(:gt, 1, ['N', 'S'], &:>)
    def_filter(:begins_with, 1, ['S'], &:start_with?)
    def_filter(:between, 2, ['N', 'S'], &:between?)
  end
end
