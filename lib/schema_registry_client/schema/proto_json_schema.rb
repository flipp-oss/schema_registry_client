# frozen_string_literal: true

require "schema_registry_client/schema/base"
require "schema_registry_client/output/json_schema"

module SchemaRegistry
  module Schema
    class ProtoJsonSchema < Base
      def self.schema_type
        "JSON"
      end

      def schema_text(message, schema_name: nil)
        SchemaRegistry::Output::JsonSchema.output(message.class.descriptor.to_proto)
      end

      def encode(message, stream, schema_name: nil)
        json = message.to_h.sort.to_h.to_json
        stream.write(json)
      end

      def decode(stream, _schema_text)
        json = stream.read
        JSON.parse(json)
      end
    end
  end
end
