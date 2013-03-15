require 'benchmark'
require 'FileUtils'
require 'timeout'


SEPARATOR_ = File::ALT_SEPARATOR || File::SEPARATOR


class String
  def to_bool
    return true if self == true || self =~ (/(true|t|yes|y|1)$/i)
    return false if self == false || self =~ (/(false|f|no|n|0)$/i)
    raise ArgumentError.new("invalid value for Boolean: \"#{self}\"")
  end
end


def shutdown
  puts 'Shutting down..'
  exit(0)
end

MENCODER_MPG_CMD = 'mencoder -forceidx -tskeepbroken -oac lavc -ovc lavc -of mpeg -mpegopts format=xvcd -vf fixpts,scale=352:288 -srate 44100 -af lavcresample=44100 -lavcopts vcodec=mpeg1video:keyint=15:vrc_buf_size=327:vrc_minrate=1152:vbitrate=1152:vrc_maxrate=1152:acodec=mp2:abitrate=224:aspect=16/9:threads=4:turbo -ofps 25 -o %s %s 2>&1 >> %s'
MENCODER_FLV_CMD = 'mencoder -forceidx -of lavf -oac mp3lame -lameopts abr:br=56 -srate 22050 -ovc lavc -lavcopts vcodec=flv:vbitrate=250:mbd=2:mv0:trell:v4mv:cbp:last_pred=3 -o %s %s 2>&1 >> %s'

Signal.trap 'SIGTERM' do
  shutdown
end

TS_FILE_HEADER = 'tsdump'

def fail_and_shutdown(message)
  puts "#{@properties_filename} :  #{message}"
  shutdown
end

class Dvbr3


  def check_config_and_process
    if @config.size == 0
      fail_and_shutdown 'Configuration is empty'
    else
      @tuner_id = "#{@config['TUNERID']}".to_i
      @channel_name = @config['CHANNELNAME']
      @audio_pid = @config['AID']
      @video_pid = @config['VID']
      @mpg_path = @config['RECPATH']
      @flv_enabled = @config['FLV_ENABLED'].to_bool
      @flv_path = @config['FLV_RECPATH']
      @lookup_path = @config['LOOKUP_PATH']

      missing_txt = 'Missing %s in configuration file'

      fail_and_shutdown(missing_txt % %w('TUNERID')) unless @tuner_id
      fail_and_shutdown(missing_txt % %w('CHANNELNAME')) unless @channel_name
      fail_and_shutdown(missing_txt % %w('AID')) unless @audio_pid
      fail_and_shutdown(missing_txt % %w('VID')) unless @video_pid
      fail_and_shutdown(missing_txt % %w('RECPATH')) unless @mpg_path
      fail_and_shutdown(missing_txt % %w('FLV_ENABLED')) if @flv_enabled == nil
      fail_and_shutdown(missing_txt % %w('FLV_RECPATH')) unless @flv_path
      fail_and_shutdown(missing_txt % %w('LOOKUP_PATH')) unless @lookup_path

      not_exists_text = '%s is not exists'

      fail_and_shutdown(not_exists_text % [@mpg_path]) unless File.exist?(@mpg_path)
      fail_and_shutdown(not_exists_text % [@flv_path]) unless File.exist?(@flv_path)
      fail_and_shutdown(not_exists_text % [@lookup_path]) unless File.exist?(@lookup_path)


      not_writable_text = '%s is not writable'

      fail_and_shutdown(not_writable_text % [@mpg_path]) unless File.writable?(@mpg_path)
      fail_and_shutdown(not_writable_text % [@flv_path]) unless File.writable?(@flv_path)
    end

  end

  TIMEOUT_1_HOUR = 60 * 60

  def run_limited(cmd, timeout)

    begin
      puts cmd
      pipe = IO.popen(cmd)

      puts "pid: #{pipe.pid}"
      Timeout.timeout(timeout) do


        Process.wait pipe.pid
      end
    rescue Timeout::TimeoutError
      puts 'Timeout'
      Process.kill 9, pipe.pid
      Process.wait pipe.pid # we need to collect status so it doesn't stick around as zombie process
      return false
    rescue Exception => ex
      puts ex.message
      return false
    end

    true


  end

  def create_metadata_for_flv(in_file)

    puts "#{@channel_name} (FLV_MD) : Generating metadata for #{in_file}"

    Benchmark.bm do |x|

      x.report('FLV_MD:') {

        run_limited("java -cp .:logback-classic-0.9.8.jar:logback-core-0.9.8.jar:mina-core-1.1.6.jar:red5.jar:slf4j-api-1.4.3.jar:xercesImpl-2.9.0.jar MetaGenerate #{in_file}", 60 * 10)
      }
    end


    puts "#{@channel_name} (FLV_MD) : Finished generating metadata for #{in_file}"
  end

  def create_flv(in_file, out_file)

    @ret = false


    puts "#{@channel_name} (FLV) : Started encoding #{in_file} to #{out_file}"

    command = MENCODER_FLV_CMD % [out_file, in_file, "#{out_file}.flvenc.log.txt"]

    Benchmark.bm do |x|

      x.report('FLV:') {

        @ret = run_limited(command, TIMEOUT_1_HOUR)


      }
    end

    puts "#{@channel_name} (FLV) : Finished encoding #{in_file} to #{out_file}"

    @ret

  end


  def create_mpg(in_file, out_file)

    @ret = false


    puts "#{@channel_name} (MPG) : Encoding #{in_file} to #{out_file}"

    command = MENCODER_MPG_CMD % [out_file, in_file, "#{out_file}.encmpg.log.txt"]

    Benchmark.bm do |x|

      x.report('MPG:') {

        @ret = run_limited(command, TIMEOUT_1_HOUR)


      }
    end


    puts "#{@channel_name} (MPG) : Finished encoding #{in_file} to #{out_file}"

    @ret

  end

  def pre_process(in_file)

    @ret = false

    puts "#{@channel_name} : Preprocessing #{in_file}"

    Benchmark.bm do |x|

      x.report('PRE: ') {

        @ret = run_limited("java -jar projectx.jar -tots -id #{@audio_pid},#{@video_pid} #{in_file} 2>&1 >> #{in_file}.pre.log.txt ", TIMEOUT_1_HOUR)

      }

    end

    @ret

  end

  def ignore_file(file_name)
    begin
      FileUtils.mv file_name, "#{file_name}.ignored"
    rescue Exception => e
      puts e.message
    end

  end

  def run
    while true

      Dir.foreach(@lookup_path) do |item|
        next if item == '.' or item == '..'

        full_path = "#{@lookup_path}#{SEPARATOR_}#{item}"

        match = item.match(/#{TS_FILE_HEADER}_(\d+)_(\d+)-(\d+).ts$/i)

        if match
          c_tuner_id = "#{match.captures[0]}".to_i
          c_start_time = "#{match.captures[1]}".to_i
          c_stop_time = "#{match.captures[2]}".to_i

          if c_tuner_id == @tuner_id

            calendar_st = Time.at(c_start_time / 1000)

            calendar_en = Time.at(c_stop_time / 1000)

            st_year = calendar_st.strftime('%Y')
            st_month = calendar_st.strftime('%m')
            st_day = calendar_st.strftime('%d')

            out_fn_body = '-%s%02d%02d_%02d%02d%02d-%02d%02d%02d' %
                [st_year,
                 st_month,
                 st_day,
                 calendar_st.strftime('%H'),
                 calendar_st.strftime('%M'),
                 calendar_st.strftime('%S'),
                 calendar_en.strftime('%H'),
                 calendar_en.strftime('%M'),
                 calendar_en.strftime('%S')                ]

            out_mpg_fn = "CH%s#{out_fn_body}.mpg" % [c_tuner_id]
            out_flv_fn = "%s#{out_fn_body}.flv" % [@channel_name]

            unless pre_process(full_path)
              ignore_file(full_path)
              next
            end

            mpg_out_path = "#{@lookup_path}#{SEPARATOR_}#{out_mpg_fn}"
            flv_out_path = "#{@lookup_path}#{SEPARATOR_}#{out_flv_fn}"

            unless create_mpg("#{full_path}_remux.ts", mpg_out_path)
              ignore_file full_path
              next
            end


            if @flv_enabled

              if File.exist?(mpg_out_path)
                unless create_flv(mpg_out_path, flv_out_path)
                  ignore_file full_path
                  next
                end


              else
                puts "#{@channel_name} : FATAL ERROR #{mpg_out_path} could not found!!!"
              end


            end

            if  File.exist? mpg_out_path


              if File.exist? flv_out_path and @flv_enabled

                create_metadata_for_flv(flv_out_path)

                flv_dest_path = "%s#{SEPARATOR_}%s#{SEPARATOR_}%02d#{SEPARATOR_}%02d#{SEPARATOR_}" % [@flv_path, st_year, st_month, st_day]

                FileUtils.mkdir_p flv_dest_path
              end

              begin
                puts "#{@channel_name} : Moving #{flv_out_path}" if @flv_enabled
                Benchmark.bm do |x|

                  x.report('MOVE_FLV: ') {
                    FileUtils.mv(flv_out_path, "#{flv_dest_path}#{SEPARATOR_}#{out_flv_fn}") if @flv_enabled
                    FileUtils.cp(flv_out_path+'.meta', "#{flv_dest_path}#{SEPARATOR_}#{out_flv_fn}.meta") if @flv_enabled


                  }
                end

                puts "#{@channel_name} : Moving #{mpg_out_path}"
                Benchmark.bm do |x|

                  x.report('MOVE_MPG: ') {
                    FileUtils.mv(mpg_out_path, "#{@mpg_path}#{SEPARATOR_}#{out_mpg_fn}")
                  }
                end


                puts "#{@channel_name} : Removing #{full_path}"
                File.delete full_path

                puts "#{@channel_name} : Removing #{full_path}_remux.ts"
                File.delete "#{full_path}_remux.ts"


              rescue Exception => ex
                puts 'An error occurred'

              end


            end


          end


        end



      end
      sleep 1.0
    end
  end

  def initialize(properties_filename)
    @properties_filename = properties_filename
    @config = load_properties(@properties_filename)

    check_config_and_process

    run

  end

  def load_properties(properties_filename)
    properties = {}
    File.open(properties_filename, 'r') do |properties_file|
      properties_file.read.each_line do |line|
        line.strip!
        if line[0] != ?# and line[0] != ?=
          i = line.index('=')
          if i
            properties[line[0..i - 1].strip] = line[i + 1..-1].strip
          else
            properties[line] = ''
          end
        end
      end
    end
    properties
  end

end

fail_and_shutdown 'Please enter a configuration file name as parameter' unless ARGV[0]

fail_and_shutdown '%s does not exists' % [ARGV[0]] unless File.exist? ARGV[0]


puts "Started on #{ARGV[0]}"

dvbr = Dvbr3.new(ARGV[0])


#start = Java::NetSourceforgeDvbProjectxCommon::Start

#start.main(['-tots','-id','166,165', '/Users/marcus/tmp/tsdump_0_1362837602097-1362844802019.ts'])





