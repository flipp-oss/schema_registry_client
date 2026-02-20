# frozen_string_literal: true

RSpec.describe "decoding" do
  let(:schema_registry_client) do
    SchemaRegistry::Client.new(
      registry_url: "http://localhost:8081"
    )
  end

  it "should decode a simple message" do
    schema = File.read("#{__dir__}/schemas/simple/simple.proto")
    stub = stub_request(:get, "http://localhost:8081/schemas/ids/15")
      .to_return_json(body: {schema: schema})
    msg = Simple::V1::SimpleMessage.new(name: "my name")
    encoded = "\u0000\u0000\u0000\u0000\u000F\u0000#{msg.to_proto}"
    expect(schema_registry_client.decode(encoded)).to eq(msg)

    # if we do it again we should not see any more requests
    expect(schema_registry_client.decode(encoded)).to eq(msg)

    expect(stub).to have_been_requested.once
  end

  it "should decode a complex message" do
    schema = File.read("#{__dir__}/schemas/referenced/referer.proto")
    stub = stub_request(:get, "http://localhost:8081/schemas/ids/20")
      .to_return_json(body: {schema: schema})
    msg = Referenced::V1::MessageB::MessageBA.new(
      simple: Simple::V1::SimpleMessage.new(name: "my name")
    )
    encoded = "\u0000\u0000\u0000\u0000\u0014\u0004\u0002\u0000#{msg.to_proto}"
    expect(schema_registry_client.decode(encoded)).to eq(msg)

    # if we do it again we should not see any more requests
    expect(schema_registry_client.decode(encoded)).to eq(msg)
    expect(stub).to have_been_requested.once
  end

  describe "with JSON" do
    let(:schema_registry_client) do
      SchemaRegistry::Client.new(
        registry_url: "http://localhost:8081",
        schema_type: SchemaRegistry::Schema::ProtoJsonSchema.new
      )
    end

    it "should decode a simple message" do
      schema = File.read("#{__dir__}/schemas/simple/simple.json")
      stub = stub_request(:get, "http://localhost:8081/schemas/ids/15")
        .to_return_json(body: {schema: schema})
      encoded = "\u0000\u0000\u0000\u0000\u000F{\"name\":\"my name\"}"
      msg = {"name" => "my name"}
      expect(schema_registry_client.decode(encoded)).to eq(msg)

      # if we do it again we should not see any more requests
      expect(schema_registry_client.decode(encoded)).to eq(msg)

      expect(stub).to have_been_requested.once
    end
  end

  describe "with Avro" do
    around(:each) do |ex|
      SchemaRegistry.avro_schema_path = "#{__dir__}/schemas"
      ex.run
      SchemaRegistry.avro_schema_path = nil
    end

    let(:schema_registry_client) do
      SchemaRegistry::Client.new(
        registry_url: "http://localhost:8081",
        schema_type: SchemaRegistry::Schema::Avro.new
      )
    end

    it "should decode a simple message" do
      schema = File.read("#{__dir__}/schemas/simple/v1/SimpleMessage.avsc")
      stub = stub_request(:get, "http://localhost:8081/schemas/ids/15")
        .to_return_json(body: {schema: schema})

      # Avro-encoded data: "my name" as string (length 0x0E + bytes)
      encoded = "\u0000\u0000\u0000\u0000\u000F\u000Emy name"
      decoded = schema_registry_client.decode(encoded)

      expect(decoded).to eq({"name" => "my name"})

      # if we do it again we should not see any more requests
      expect(schema_registry_client.decode(encoded)).to eq(decoded)

      expect(stub).to have_been_requested.once
    end

    it "should decode a complex message with nested record" do
      schema = File.read("#{__dir__}/schemas/referenced/v1/MessageBA.avsc")
      stub = stub_request(:get, "http://localhost:8081/schemas/ids/20")
        .to_return_json(body: {schema: schema})

      # Avro-encoded nested record
      encoded = "\u0000\u0000\u0000\u0000\u0014\u000Emy name"
      decoded = schema_registry_client.decode(encoded)

      expect(decoded).to eq({
        "simple" => {
          "name" => "my name"
        }
      })

      # if we do it again we should not see any more requests
      expect(schema_registry_client.decode(encoded)).to eq(decoded)
      expect(stub).to have_been_requested.once
    end

    it "should decode a message with multiple fields" do
      multi_schema = {
        "type" => "record",
        "name" => "MultiFieldMessage",
        "namespace" => "test.v1",
        "fields" => [
          {"name" => "name", "type" => "string"},
          {"name" => "age", "type" => "int"}
        ]
      }
      schema_json = JSON.generate(multi_schema)

      stub = stub_request(:get, "http://localhost:8081/schemas/ids/25")
        .to_return_json(body: {schema: schema_json})

      # Manually encode the message for testing
      # Alice = 0x0A (length 5*2) + "Alice" bytes
      # age 30 = zigzag encoded as 60 (0x3C)
      encoded = "\u0000\u0000\u0000\u0000\u0019\u000AAlice\u003C"
      decoded = schema_registry_client.decode(encoded)

      expect(decoded).to eq({"name" => "Alice", "age" => 30})

      expect(stub).to have_been_requested.once
    end

    it "should handle schema evolution with reader schema" do
      # Writer schema (what was used to encode) - has an additional field with default
      writer_schema = {
        "type" => "record",
        "name" => "SimpleMessage",
        "namespace" => "simple.v1",
        "fields" => [
          {"name" => "name", "type" => "string"},
          {"name" => "age", "type" => "int", "default" => 0}
        ]
      }

      # Reader schema (what we have locally) - doesn't have the age field
      # This simulates reading old data with a newer schema or vice versa

      stub = stub_request(:get, "http://localhost:8081/schemas/ids/15")
        .to_return_json(body: {schema: JSON.generate(writer_schema)})

      # Encoded with writer schema: "my name" (0x0E + bytes) + age 25 (zigzag encoded as 50 = 0x32)
      encoded = "\u0000\u0000\u0000\u0000\u000F\u000Emy name\u0032"
      decoded = schema_registry_client.decode(encoded)

      # Decoded value should only have 'name' from reader schema, age is ignored
      expect(decoded).to eq({"name" => "my name"})
      expect(stub).to have_been_requested.once
    end

    it "should raise error for invalid magic byte" do
      # Wrong magic byte (0x01 instead of 0x00)
      encoded = "\u0001\u0000\u0000\u0000\u000F\u000Emy name"

      expect do
        schema_registry_client.decode(encoded)
      end.to raise_error(/Expected data to begin with a magic byte/)
    end

    it "should raise error for unknown schema id" do
      # Schema ID 999 is not stubbed, so decoding should fail
      encoded = "\u0000\u0000\u0000\u0003\u00E7\u000Emy name"

      expect do
        schema_registry_client.decode(encoded)
      end.to raise_error(/Schema|not found/i)
    end
  end

  describe "caching" do
    it "should not fetch the same schema ID twice (Protobuf)" do
      schema = File.read("#{__dir__}/schemas/simple/simple.proto")
      stub = stub_request(:get, "http://localhost:8081/schemas/ids/15")
        .to_return_json(body: {schema: schema})
      msg = Simple::V1::SimpleMessage.new(name: "my name")
      encoded = "\u0000\u0000\u0000\u0000\u000F\u0000#{msg.to_proto}"

      3.times { schema_registry_client.decode(encoded) }

      expect(stub).to have_been_requested.once
    end

    it "should fetch different schema IDs independently" do
      simple_schema = File.read("#{__dir__}/schemas/simple/simple.proto")
      stub_15 = stub_request(:get, "http://localhost:8081/schemas/ids/15")
        .to_return_json(body: {schema: simple_schema})

      ref_schema = File.read("#{__dir__}/schemas/referenced/referer.proto")
      stub_20 = stub_request(:get, "http://localhost:8081/schemas/ids/20")
        .to_return_json(body: {schema: ref_schema})

      msg1 = Simple::V1::SimpleMessage.new(name: "my name")
      encoded1 = "\u0000\u0000\u0000\u0000\u000F\u0000#{msg1.to_proto}"

      msg2 = Referenced::V1::MessageB::MessageBA.new(
        simple: Simple::V1::SimpleMessage.new(name: "my name")
      )
      encoded2 = "\u0000\u0000\u0000\u0000\u0014\u0004\u0002\u0000#{msg2.to_proto}"

      # Decode both messages
      schema_registry_client.decode(encoded1)
      schema_registry_client.decode(encoded2)

      # Each schema fetched exactly once
      expect(stub_15).to have_been_requested.once
      expect(stub_20).to have_been_requested.once

      # Decode again - no new requests
      schema_registry_client.decode(encoded1)
      schema_registry_client.decode(encoded2)

      expect(stub_15).to have_been_requested.once
      expect(stub_20).to have_been_requested.once
    end

    it "should not share cache between different client instances" do
      schema = File.read("#{__dir__}/schemas/simple/simple.proto")
      stub = stub_request(:get, "http://localhost:8081/schemas/ids/15")
        .to_return_json(body: {schema: schema})
      msg = Simple::V1::SimpleMessage.new(name: "my name")
      encoded = "\u0000\u0000\u0000\u0000\u000F\u0000#{msg.to_proto}"

      schema_registry_client.decode(encoded)

      # A second client should make its own request
      client2 = SchemaRegistry::Client.new(registry_url: "http://localhost:8081")
      client2.decode(encoded)

      expect(stub).to have_been_requested.twice
    end

    describe "with Avro" do
      around(:each) do |ex|
        SchemaRegistry.avro_schema_path = "#{__dir__}/schemas"
        ex.run
        SchemaRegistry.avro_schema_path = nil
      end

      let(:schema_registry_client) do
        SchemaRegistry::Client.new(
          registry_url: "http://localhost:8081",
          schema_type: SchemaRegistry::Schema::Avro.new
        )
      end

      it "should not fetch the same schema ID twice" do
        schema = File.read("#{__dir__}/schemas/simple/v1/SimpleMessage.avsc")
        stub = stub_request(:get, "http://localhost:8081/schemas/ids/15")
          .to_return_json(body: {schema: schema})

        encoded = "\u0000\u0000\u0000\u0000\u000F\u000Emy name"

        3.times { schema_registry_client.decode(encoded) }

        expect(stub).to have_been_requested.once
      end

      it "should fetch different schema IDs independently" do
        simple_schema = File.read("#{__dir__}/schemas/simple/v1/SimpleMessage.avsc")
        stub_15 = stub_request(:get, "http://localhost:8081/schemas/ids/15")
          .to_return_json(body: {schema: simple_schema})

        nested_schema = File.read("#{__dir__}/schemas/referenced/v1/MessageBA.avsc")
        stub_20 = stub_request(:get, "http://localhost:8081/schemas/ids/20")
          .to_return_json(body: {schema: nested_schema})

        encoded1 = "\u0000\u0000\u0000\u0000\u000F\u000Emy name"
        encoded2 = "\u0000\u0000\u0000\u0000\u0014\u000Emy name"

        # Decode both messages
        schema_registry_client.decode(encoded1)
        schema_registry_client.decode(encoded2)

        expect(stub_15).to have_been_requested.once
        expect(stub_20).to have_been_requested.once

        # Decode again - no new requests
        schema_registry_client.decode(encoded1)
        schema_registry_client.decode(encoded2)

        expect(stub_15).to have_been_requested.once
        expect(stub_20).to have_been_requested.once
      end
    end
  end
end
