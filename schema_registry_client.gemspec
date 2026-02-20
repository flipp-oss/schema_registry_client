# frozen_string_literal: true

lib = File.expand_path("lib", __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "schema_registry_client/version"

Gem::Specification.new do |spec|
  spec.name = "schema_registry_client"
  spec.version = SchemaRegistry::VERSION
  spec.authors = ["Daniel Orner"]
  spec.email = ["daniel.orner@flipp.com"]
  spec.summary = "Confluent Schema Registry client with support for Avro and Protobuf"
  spec.homepage = "https://github.com/flipp-oss/schema_registry_client"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.2"

  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = `git ls-files -z`.split("\x0")
  spec.executables = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "avro"
  spec.add_dependency "excon"
  spec.add_dependency "google-protobuf"

  spec.add_development_dependency "bundler", "~> 2.0"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.2"
  spec.add_development_dependency "simplecov"
  spec.add_development_dependency "standardrb"
  spec.add_development_dependency "webmock"
end
