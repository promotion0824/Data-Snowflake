/* 
 ------------------------------------------------------------------------------------------------------------------------------
  This function converts between timezone format:
  https://docs.microsoft.com/en-us/windows-hardware/manufacture/desktop/default-time-zones?view=windows-11
  used by Windows systems and "tz database" format used by Snowflake: 
  https://en.wikipedia.org/wiki/List_of_tz_database_time_zones.
  
  Ideally we would get this dynamically updated but this should suffice for now.
  The function returns 'Unknown' if mapping is not found and this results in an error when used as a parameter
  in function such as CONVERT_TIMEZONE.
 ------------------------------------------------------------------------------------------------------------------------------
*/
CREATE OR REPLACE FUNCTION utils.convert_time_zone_name_from_windows_to_tzdata_udf(time_zone_name STRING)
  RETURNS STRING
  AS
  $$
    SELECT
      CASE time_zone_name
        WHEN 'AUS Central Standard Time' THEN 'Australia/Darwin' 
        WHEN 'AUS Eastern Standard Time' THEN 'Australia/Sydney' 
        WHEN 'Afghanistan Standard Time' THEN 'Asia/Kabul' 
        WHEN 'Alaskan Standard Time' THEN 'America/Anchorage' 
        WHEN 'Aleutian Standard Time' THEN 'America/Adak' 
        WHEN 'Altai Standard Time' THEN 'Asia/Barnaul' 
        WHEN 'Arab Standard Time' THEN 'Asia/Riyadh' 
        WHEN 'Arabian Standard Time' THEN 'Asia/Dubai' 
        WHEN 'Arabic Standard Time' THEN 'Asia/Baghdad' 
        WHEN 'Argentina Standard Time' THEN 'America/Argentina/Buenos_Aires' 
        WHEN 'Astrakhan Standard Time' THEN 'Europe/Astrakhan' 
        WHEN 'Atlantic Standard Time' THEN 'America/Halifax' 
        WHEN 'Aus Central W. Standard Time' THEN 'Australia/Eucla' 
        WHEN 'Azerbaijan Standard Time' THEN 'Asia/Baku' 
        WHEN 'Azores Standard Time' THEN 'Atlantic/Azores' 
        WHEN 'Bahia Standard Time' THEN 'America/Bahia' 
        WHEN 'Bangladesh Standard Time' THEN 'Asia/Dhaka' 
        WHEN 'Belarus Standard Time' THEN 'Europe/Minsk' 
        WHEN 'Bougainville Standard Time' THEN 'Pacific/Bougainville' 
        WHEN 'Canada Central Standard Time' THEN 'America/Regina' 
        WHEN 'Cape Verde Standard Time' THEN 'Atlantic/Cape_Verde' 
        WHEN 'Caucasus Standard Time' THEN 'Asia/Yerevan' 
        WHEN 'Cen. Australia Standard Time' THEN 'Australia/Adelaide' 
        WHEN 'Central America Standard Time' THEN 'America/Guatemala' 
        WHEN 'Central Asia Standard Time' THEN 'Asia/Almaty' 
        WHEN 'Central Brazilian Standard Time' THEN 'America/Cuiaba' 
        WHEN 'Central Europe Standard Time' THEN 'Europe/Budapest' 
        WHEN 'Central European Standard Time' THEN 'Europe/Warsaw' 
        WHEN 'Central Pacific Standard Time' THEN 'Pacific/Guadalcanal' 
        WHEN 'Central Standard Time (Mexico)' THEN 'America/Mexico_City' 
        WHEN 'Central Standard Time' THEN 'America/Chicago' 
        WHEN 'Chatham Islands Standard Time' THEN 'Pacific/Chatham' 
        WHEN 'China Standard Time' THEN 'Asia/Shanghai' 
        WHEN 'Cuba Standard Time' THEN 'America/Havana' 
        WHEN 'Dateline Standard Time' THEN 'Etc/GMT+12' 
        WHEN 'E. Africa Standard Time' THEN 'Africa/Nairobi' 
        WHEN 'E. Australia Standard Time' THEN 'Australia/Brisbane' 
        WHEN 'E. Europe Standard Time' THEN 'Europe/Chisinau' 
        WHEN 'E. South America Standard Time' THEN 'America/Sao_Paulo' 
        WHEN 'Easter Island Standard Time' THEN 'Pacific/Easter' 
        WHEN 'Eastern Standard Time (Mexico)' THEN 'America/Cancun' 
        WHEN 'Eastern Standard Time' THEN 'America/New_York' 
        WHEN 'Egypt Standard Time' THEN 'Africa/Cairo' 
        WHEN 'Ekaterinburg Standard Time' THEN 'Asia/Yekaterinburg' 
        WHEN 'FLE Standard Time' THEN 'Europe/Kiev' 
        WHEN 'Fiji Standard Time' THEN 'Pacific/Fiji' 
        WHEN 'GMT Standard Time' THEN 'Europe/London' 
        WHEN 'GTB Standard Time' THEN 'Europe/Bucharest' 
        WHEN 'Georgian Standard Time' THEN 'Asia/Tbilisi' 
        WHEN 'Greenland Standard Time' THEN 'America/Godthab' 
        WHEN 'Greenwich Standard Time' THEN 'Atlantic/Reykjavik' 
        WHEN 'Haiti Standard Time' THEN 'America/Port-au-Prince' 
        WHEN 'Hawaiian Standard Time' THEN 'Pacific/Honolulu' 
        WHEN 'India Standard Time' THEN 'Asia/Kolkata' 
        WHEN 'Iran Standard Time' THEN 'Asia/Tehran' 
        WHEN 'Israel Standard Time' THEN 'Asia/Jerusalem' 
        WHEN 'Jordan Standard Time' THEN 'Asia/Amman' 
        WHEN 'Kaliningrad Standard Time' THEN 'Europe/Kaliningrad' 
        WHEN 'Kamchatka Standard Time' THEN 'Asia/Kamchatka' 
        WHEN 'Korea Standard Time' THEN 'Asia/Seoul' 
        WHEN 'Libya Standard Time' THEN 'Africa/Tripoli' 
        WHEN 'Line Islands Standard Time' THEN 'Pacific/Kiritimati' 
        WHEN 'Lord Howe Standard Time' THEN 'Australia/Lord_Howe' 
        WHEN 'Magadan Standard Time' THEN 'Asia/Magadan' 
        WHEN 'Marquesas Standard Time' THEN 'Pacific/Marquesas' 
        WHEN 'Mauritius Standard Time' THEN 'Indian/Mauritius' 
        WHEN 'Mid-Atlantic Standard Time' THEN 'Etc/GMT+2' 
        WHEN 'Middle East Standard Time' THEN 'Asia/Beirut' 
        WHEN 'Montevideo Standard Time' THEN 'America/Montevideo' 
        WHEN 'Morocco Standard Time' THEN 'Africa/Casablanca' 
        WHEN 'Mountain Standard Time (Mexico)' THEN 'America/Chihuahua' 
        WHEN 'Mountain Standard Time' THEN 'America/Denver' 
        WHEN 'Myanmar Standard Time' THEN 'Asia/Yangon' 
        WHEN 'N. Central Asia Standard Time' THEN 'Asia/Novosibirsk' 
        WHEN 'Namibia Standard Time' THEN 'Africa/Windhoek' 
        WHEN 'Nepal Standard Time' THEN 'Asia/Kathmandu' 
        WHEN 'New Zealand Standard Time' THEN 'Pacific/Auckland' 
        WHEN 'Newfoundland Standard Time' THEN 'America/St_Johns' 
        WHEN 'Norfolk Standard Time' THEN 'Pacific/Norfolk' 
        WHEN 'North Asia East Standard Time' THEN 'Asia/Irkutsk' 
        WHEN 'North Asia Standard Time' THEN 'Asia/Krasnoyarsk' 
        WHEN 'North Korea Standard Time' THEN 'Asia/Pyongyang' 
        WHEN 'Omsk Standard Time' THEN 'Asia/Omsk' 
        WHEN 'Pacific SA Standard Time' THEN 'America/Santiago' 
        WHEN 'Pacific Standard Time (Mexico)' THEN 'America/Tijuana' 
        WHEN 'Pacific Standard Time' THEN 'America/Los_Angeles' 
        WHEN 'Pakistan Standard Time' THEN 'Asia/Karachi' 
        WHEN 'Paraguay Standard Time' THEN 'America/Asuncion' 
        WHEN 'Romance Standard Time' THEN 'Europe/Paris' 
        WHEN 'Russia Time Zone 10' THEN 'Asia/Srednekolymsk' 
        WHEN 'Russia Time Zone 11' THEN 'Asia/Kamchatka' 
        WHEN 'Russia Time Zone 3' THEN 'Europe/Samara' 
        WHEN 'Russian Standard Time' THEN 'Europe/Moscow' 
        WHEN 'SA Eastern Standard Time' THEN 'America/Cayenne' 
        WHEN 'SA Pacific Standard Time' THEN 'America/Bogota' 
        WHEN 'SA Western Standard Time' THEN 'America/La_Paz' 
        WHEN 'SE Asia Standard Time' THEN 'Asia/Bangkok' 
        WHEN 'Saint Pierre Standard Time' THEN 'America/Miquelon' 
        WHEN 'Sakhalin Standard Time' THEN 'Asia/Sakhalin' 
        WHEN 'Samoa Standard Time' THEN 'Pacific/Apia' 
        WHEN 'Singapore Standard Time' THEN 'Asia/Singapore' 
        WHEN 'South Africa Standard Time' THEN 'Africa/Johannesburg' 
        WHEN 'Sri Lanka Standard Time' THEN 'Asia/Colombo' 
        WHEN 'Syria Standard Time' THEN 'Asia/Damascus' 
        WHEN 'Taipei Standard Time' THEN 'Asia/Taipei' 
        WHEN 'Tasmania Standard Time' THEN 'Australia/Hobart' 
        WHEN 'Tocantins Standard Time' THEN 'America/Araguaina' 
        WHEN 'Tokyo Standard Time' THEN 'Asia/Tokyo' 
        WHEN 'Tomsk Standard Time' THEN 'Asia/Tomsk' 
        WHEN 'Tonga Standard Time' THEN 'Pacific/Tongatapu' 
        WHEN 'Transbaikal Standard Time' THEN 'Asia/Chita' 
        WHEN 'Turkey Standard Time' THEN 'Europe/Istanbul' 
        WHEN 'Turks And Caicos Standard Time' THEN 'America/Grand_Turk' 
        WHEN 'US Eastern Standard Time' THEN 'America/Indiana/Indianapolis' 
        WHEN 'US Mountain Standard Time' THEN 'America/Phoenix' 
        WHEN 'UTC+12' THEN 'Etc/GMT-12' 
        WHEN 'UTC' THEN 'Etc/UTC' 
        WHEN 'UTC-02' THEN 'Etc/GMT+2' 
        WHEN 'UTC-08' THEN 'Etc/GMT+8' 
        WHEN 'UTC-09' THEN 'Etc/GMT+9' 
        WHEN 'UTC-11' THEN 'Etc/GMT+11' 
        WHEN 'Ulaanbaatar Standard Time' THEN 'Asia/Ulaanbaatar' 
        WHEN 'Venezuela Standard Time' THEN 'America/Caracas' 
        WHEN 'Vladivostok Standard Time' THEN 'Asia/Vladivostok' 
        WHEN 'W. Australia Standard Time' THEN 'Australia/Perth' 
        WHEN 'W. Central Africa Standard Time' THEN 'Africa/Lagos' 
        WHEN 'W. Europe Standard Time' THEN 'Europe/Berlin' 
        WHEN 'W. Mongolia Standard Time' THEN 'Asia/Hovd' 
        WHEN 'West Asia Standard Time' THEN 'Asia/Tashkent' 
        WHEN 'West Bank Standard Time' THEN 'Asia/Hebron' 
        WHEN 'West Pacific Standard Time' THEN 'Pacific/Port_Moresby' 
        WHEN 'Yakutsk Standard Time' THEN 'Asia/Yakutsk' 
        ELSE 'Unknown'
      END AS time_zone_name
  $$
  ;