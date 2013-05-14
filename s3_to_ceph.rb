#!/usr/bin/env/ruby
#Copyright (C) 2013 Joachim Nolten
#Permission is hereby granted, free of charge, to any person obtaining a copy of
#this software and associated documentation files (the "Software"), to deal in
#the Software without restriction, including without limitation the rights to
#use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
#of the Software, and to permit persons to whom the Software is furnished to do
#so, subject to the following conditions: The above copyright notice and this
#permission notice shall be included in all copies or substantial portions of
#the Software.  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO
#EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES
#OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
#ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#DEALINGS IN THE SOFTWARE.

require 'rubygems'
require 'bundler/setup'
require 'thread_safe'
require 'logger'
require 'yaml'
require 'pry'
require 'tempfile'
require 'parallel'
require 'timeout'
require 'mime-types'

module MIME
  class Types
    def initialize(data_version = nil)
      @type_variants    = ThreadSafe::Hash.new { |h, k| h[k] = [] }
      @extension_index  = ThreadSafe::Hash.new
      @extension_index['jpg'] = ThreadSafe::Array.new 
      @extension_index['jpeg'] = ThreadSafe::Array.new 
      @extension_index['png'] = ThreadSafe::Array.new 
      @extension_index['gif'] = ThreadSafe::Array.new 
      @extension_index['tiff'] = ThreadSafe::Array.new 
      @extension_index['tif'] = ThreadSafe::Array.new 
      @data_version = data_version
    end
    
    def type_for(filename, platform = false)
      ext = filename.chomp.downcase.gsub(/.*\./o, '')
      list = @extension_index.clone[ext]
      list.delete_if { |e| not e.platform? } if platform
      list
    end
  end
end

require 'fog'

# No longer necesarry for next Fog release
module Excon
  class Connection
     VALID_CONNECTION_KEYS << :url
  end
end

Excon.defaults[:ssl_verify_peer] = false

OpenSSL::SSL::VERIFY_PEER = OpenSSL::SSL::VERIFY_NONE

module S3Backup
  mylogger = Logger.new('backup.log', 'daily')
  STOR_CONF = {}

  confpath = ["#{ENV['S3CONF']}", Dir.home, "/etc/s3conf"]

  confpath.each do |path|
    if File.exists?(path) and File.directory?(path) and File.exists?("#{path}/s3config.yml")
      config = YAML.load_file("#{path}/s3config.yml")
      config.each_pair do |key, value|
        STOR_CONF[key] = value
      end
    end
  end

  s3 = Fog::Storage.new({
    :provider => 'AWS',
    :aws_access_key_id => STOR_CONF['AWS_ACCESS_KEY_ID'],
    :aws_secret_access_key => STOR_CONF['AWS_SECRET_ACCESS_KEY'],
    :region => 'eu-west-1',
    :path_style => true
  })

  ceph = Fog::Storage.new({
    :provider => 'AWS',
    :host       => 'radosgw.webminded.nl',
    :aws_access_key_id     => STOR_CONF['CEPH_ACCESS_KEY_ID'],
    :aws_secret_access_key => STOR_CONF['CEPH_SECRET_ACCESS_KEY'],
    :path_style => true,
    :scheme => 'http',
    :port => 80
  })

  ceph_dir = ceph.directories.get('images.eu.viewbook.com')
  files = s3.directories.get('images.eu.viewbook.com').files

  subset = files.all(:marker => 'fffffeacf7afed9b3de61fd3a01776f7.jpg')

  def S3Backup.parallel_copy files, target_dir, logger
    Parallel.each(files, :in_threads => 12) do |s3_file|
    next if s3_file.key.include?('/') # can't handle slashes in filenames yet
    begin
    Timeout.timeout(60) do
      logger.info(s3_file.key)
      tempfile = Tempfile.new(s3_file.key)
      try_this(3, "S3") do
        tempfile.write(s3_file.body)
      end
      try_this(3, "Ceph") do
        target_dir.files.create(:key => s3_file.key, :body => tempfile, :public => true )
      end
      tempfile.close
      tempfile.unlink
    end
    rescue Timeout::Error
      $stderr.puts("Timeout")
    end
    end
  end

  def S3Backup.try_this tries=3, error="error"
    begin
      yield
    rescue Excon::Errors::InternalServerError => e
      tries += 1
      if tries < 3
        sleep 30
        retry
      else
        $stderr.puts("#{error}: #{e.message}")
	#raise "Too many errors"
      end
    end
  end

  parallel_copy subset, ceph_dir, mylogger
  while subset.is_truncated
    s3.sync_clock
    subset = subset.all(:marker => subset.last.key)
    parallel_copy subset, ceph_dir, mylogger
  end

end
