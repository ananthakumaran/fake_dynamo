require 'spec_helper'

module FakeDynamo
  describe Item do
    subject do
      item = Item.new
      key = Key.new
      key.primary = Attribute.new('id', 'ananth', 'S')
      item.key = key
      item.attributes = {}
      item
    end

    context "#update" do
      it "should not allow to update primary key" do
        expect { subject.update('id', nil) }.to raise_error(ValidationException, /part of the key/)
      end

      it "should handle unknown action" do
        expect { subject.update('xyz', {'Action' => 'XYZ'}) }.to raise_error(ValidationException, /unknown action/i)
      end

      it "should not allow empty value for action other than delete" do
        expect { subject.update('xyz', {'Action' => 'PUT'})}.to raise_error(ValidationException, /only delete/i)
      end
    end

    context "#delete" do
      it "should not fail when the attribute is not present" do
        subject.delete('friends', nil)
        subject.attributes['friends'].should be_nil
      end

      it "should delete the attribute" do
        subject.attributes['friends'] = Attribute.new('friends', ["1", "2"], "NS")
        subject.delete('friends', nil)
        subject.attributes['friends'].should be_nil
      end

      it "should handle value type" do
        subject.attributes['friends'] = Attribute.new('friends', ["1", "2"], "NS")
        expect { subject.delete('friends', { "S" => "XYZ" }) }.to raise_error(ValidationException, /type mismatch/i)

        subject.attributes['age'] = Attribute.new('age', "5", "N")
        expect { subject.delete('age', { "N" => "10" }) }.to raise_error(ValidationException, /not supported/i)
      end

      it "should delete values" do
        subject.attributes['friends'] = Attribute.new('friends', ["1", "2"], "NS")
        subject.delete('friends', { "NS" => ["2", "4"]})
        subject.attributes['friends'].value.should == [Num.new("1")]
      end
    end

    context "#put" do
      it "should update the attribute" do
        old_name = Attribute.new('name', 'xxx', 'S')
        subject.attributes['name'] = old_name
        subject.put('name', { 'S' => 'ananth'});
        subject.attributes['name'].should eq(Attribute.new('name', 'ananth', 'S'))

        subject.attributes['xxx'].should be_nil
        subject.put('xxx', { 'S' => 'new'} )
        subject.attributes['xxx'].should eq(Attribute.new('xxx', 'new', 'S'))
      end
    end

    context "#add" do
      it "should fail on string type" do
        expect { subject.add('new', { 'S' => 'ananth'}) }.to raise_error(ValidationException, /not supported/)
      end

      it "should increment numbers" do
        subject.attributes['number'] = Attribute.new('number', '5', 'N')
        subject.add('number', { 'N' => '3'})
        subject.attributes['number'].value.should eq(Num.new('8'))
      end

      it "should decrement numbers" do
        subject.attributes['number'] = Attribute.new('number', '5', 'N')
        subject.add('number', { 'N' => '-3'})
        subject.attributes['number'].value.should eq(Num.new('2'))
      end

      it "should handle sets" do
        subject.attributes['set'] = Attribute.new('set', ['1', '2'], 'SS')
        subject.add('set', { 'SS' => ['3']})
        subject.attributes['set'].value.should eq(['1', '2', '3'])
      end

      it "should handle duplicate in sets" do
        subject.attributes['set'] = Attribute.new('set', ['1', '2'], 'SS')
        subject.add('set', { 'SS' => ['3', '2']})
        subject.attributes['set'].value.should eq(['1', '2', '3'])
      end

      it "should handle type mismatch" do
        subject.attributes['xxx'] = Attribute.new('xxx', ['1', '2'], 'NS')
        expect { subject.add('xxx', {'SS' => ['3']}) }.to raise_error(ValidationException, /type mismatch/i)
      end

      it "should add the item if attribute is not found" do
        subject.attributes['unknown'].should be_nil
        subject.add('unknown', {'SS' => ['1']})
        subject.attributes['unknown'].should eq(Attribute.new('unknown', ['1'], 'SS'))
      end
    end
  end
end
