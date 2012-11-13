require 'em-http'
require 'em-synchrony/em-http'
require 'goliath'

class Proxy < Goliath::API

  def options_parser opts, options
    options[:mainline]           = 'http://localhost:9001'
    options[:duplicate]          = 'http://localhost:9002'
    options[:duplicate_fraction] = 0.1

    opts.on("-M", "--mainline URL",  "The URL of the host to forward all traffic to and to relay all responses from.") { |val| options[:mainline]  = val      }
    opts.on("-D", "--duplicate URL", "The URL of the host to silently forward a fraction of traffic to.")              { |val| options[:duplicate] = val      }
    opts.on("-f", "--fraction FRAC", "The fraction of traffic to silently forward.")                                   { |val| options[:fraction]  = val.to_f }
  end

  def mainline_host
    @mainline_host ||= env['options'][:mainline]
  end

  def duplicate_host
    @duplicate_host ||= env['options'][:duplicate]
  end

  def duplicate_fraction
    @duplicate_fraction ||= env['options'][:fraction].to_f
  end

  # Capture the headers when they roll in, to replay for the remote target
  def on_headers(env, headers)
    env['client-headers'] = headers
  end

  def create_request_and_params(host, env)
    url    = File.join(host, env[Goliath::Request::PATH_INFO])
    req    = EM::HttpRequest.new(url)
    params = {:head => env['client-headers'], :query => env[Goliath::Request::QUERY_STRING]}
    # params[:head]['Host'] = host
    [req, params]
  end

  # Pass the call request on to the target host
  def response(env)
    env.trace :response_beg

    body = (env[Goliath::Request::RACK_INPUT].read || '') if env[Goliath::Request::REQUEST_METHOD] == "POST"

    mainline_request,  mainline_params  = create_request_and_params(mainline_host, env)
    # env.logger.debug "Forwarding to mainline URI #{mainline_request.uri}"
    mainline_response =
      case(env[Goliath::Request::REQUEST_METHOD])
      when 'GET'
        mainline_request.get(mainline_params)
      when 'POST'
        mainline_request.post(mainline_params.merge(:body => body))
      when 'HEAD'
        mainline_request.head(mainline_params)
      else raise Goliath::Validation::BadRequestError.new("Uncool method #{env[Goliath::Request::REQUEST_METHOD]}")
      end

    if duplicate = (rand() < duplicate_fraction)
      duplicate_request, duplicate_params = create_request_and_params(duplicate_host, env)
      # env.logger.debug "Forwarding to duplicate URI #{duplicate_request.uri}"
      duplicate_response =
        case(env[Goliath::Request::REQUEST_METHOD])
        when 'GET'
          duplicate_request.aget(duplicate_params)
        when 'POST'
          duplicate_request.apost(duplicate_params.merge(:body => body))
        when 'HEAD'
          duplicate_request.ahead(duplicate_params)
        else raise Goliath::Validation::BadRequestError.new("Uncool method #{env[Goliath::Request::REQUEST_METHOD]}")
        end
    end
    
    env.trace :response_end
    [mainline_response.response_header.status, response_header_hash(mainline_response), mainline_response.response]
  end

  # Need to convert from the CONTENT_TYPE we'll get back from the server
  # to the normal Content-Type header
  def response_header_hash(resp)
    hsh = {}
    resp.response_header.each_pair do |k, v|
      hsh[to_http_header(k)] = v
    end
    hsh
  end

  def to_http_header(k)
    k.downcase.split('_').map{|e| e.capitalize }.join('-')
  end
end
