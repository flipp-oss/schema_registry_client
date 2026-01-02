# frozen_string_literal: true

require "schema_registry_client/output/json_schema"

RSpec.describe SchemaRegistry::Output::JsonSchema do
  it "should output as expected" do
    output = described_class.output(Everything::V1::TestAllTypes.descriptor.to_proto)

    expected = File.read("#{__dir__}/schemas/everything/everything.json")
    expect("#{output}\n").to eq(expected)
  end
end
