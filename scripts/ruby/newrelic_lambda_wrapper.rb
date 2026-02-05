# frozen_string_literal: true

ENV['NEW_RELIC_DISTRIBUTED_TRACING_ENABLED'] ||= 'true'
ENV['AWS_LAMBDA_FUNCTION_NAME'] ||= 'lambda_function'
ENV['NEW_RELIC_APP_NAME'] ||= ENV.fetch('AWS_LAMBDA_FUNCTION_NAME', nil)
ENV['NEW_RELIC_TRUSTED_ACCOUNT_KEY'] = ENV.fetch('NEW_RELIC_ACCOUNT_ID', '')

class NewRelicLambdaWrapper
  HANDLER_VAR = 'NEW_RELIC_LAMBDA_HANDLER'
  NR_LAYER_GEM_PATH = "/opt/ruby/gems/#{RUBY_VERSION.rpartition('.').first}.0".freeze

  def self.adjust_load_path
    return unless Dir.exist?(NR_LAYER_GEM_PATH)

    # Add the gems directory to load path
    gem_dirs = Dir.glob(File.join(NR_LAYER_GEM_PATH, 'gems', '*'))
    gem_dirs.each do |gem_dir|
      lib_dir = File.join(gem_dir, 'lib')
      $LOAD_PATH.unshift(lib_dir) if Dir.exist?(lib_dir) && !$LOAD_PATH.include?(lib_dir)
    end
    
    # Also check specifications directory exists
    specs_dir = File.join(NR_LAYER_GEM_PATH, 'specifications')
    if Dir.exist?(specs_dir)
      # Add to GEM_PATH if not already there
      gem_path = ENV['GEM_PATH'] || ''
      unless gem_path.split(':').include?(NR_LAYER_GEM_PATH)
        ENV['GEM_PATH'] = [NR_LAYER_GEM_PATH, gem_path].reject(&:empty?).join(':')
      end
    end
  end

  def self.require_ruby_agent
    adjust_load_path
    require 'newrelic_rpm'
  rescue StandardError => e
    raise "#{self.class.name}: failed to require New Relic layer provided gem(s) - #{e}"
  end

  def self.method_name_and_namespace
    @method_name_and_namespace ||= parse_customer_handler_string
  rescue StandardError => e
    raise "#{self.class.name}: failed to prep the Lambda function to be wrapped - #{e}"
  end

  def self.parse_customer_handler_string
    handler_string = ENV.fetch(HANDLER_VAR, nil)
    raise "Environment variable '#{HANDLER_VAR}' is not set!" unless handler_string

    elements = handler_string.split('.')
    ridx = determine_ridx(elements)
    file = elements[0..ridx].join('.')
    method_string = elements[(ridx + 1)..].join('.')

    require_source_file(file)

    method_string.split('.').reverse
  end
  private_class_method :parse_customer_handler_string

  def self.determine_ridx(elements)
    if elements.size == 1
      raise "Failed to parse the '#{HANDLER_VAR}' env var which is expected to be in '<path>.<method>' format!"
    end

    elements.size > 2 ? -3 : -2
  end
  private_class_method :determine_ridx

  def self.require_source_file(path)
    path = "#{path}.rb" unless path.end_with?('.rb')
    path = "#{Dir.pwd}/#{path}" unless path.start_with?('/')
    raise "Path '#{path}' does not exist or is not readable" unless File.exist?(path) && File.readable?(path)

    require_relative path
  end
  private_class_method :require_source_file
end

NewRelicLambdaWrapper.method_name_and_namespace
NewRelicLambdaWrapper.require_ruby_agent

def handler(event:, context:)
  method_name, namespace = NewRelicLambdaWrapper.method_name_and_namespace
  NewRelic::Agent.agent.serverless_handler.invoke_lambda_function_with_new_relic(event:,
                                                                                 context:,
                                                                                 method_name:,
                                                                                 namespace:)
end
