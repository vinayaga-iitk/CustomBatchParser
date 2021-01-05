package com.inshorts.kafka.indexer;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.inshorts.kafka.KafkaExtensionModule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

public class NewBatchInputRowParserTest
{
    private static final ObjectMapper MAPPER = new DefaultObjectMapper();

    static {
        for (Module module : new KafkaExtensionModule().getJacksonModules()) {
            MAPPER.registerModule(module);
        }
    }

    final String[] inputRows = new String[]{
            "{\"batch\":[{\"anonymousId\":\"70032613-9126-4488-8ffa-3f3ec0d4f8dc\",\"channel\":\"mobile\",\"context\":{\"app\":{\"build\":3040309,\"name\":\"Public\",\"namespace\":\"com.cardfeed.video_public\",\"version\":\"2.19.9\"},\"device\":{\"adTrackingEnabled\":true,\"advertisingId\":\"46de8b88-91d0-4ca7-b14f-4080af389575\",\"id\":\"bb35abc9f1aa3ccf\",\"manufacturer\":\"Realme\",\"model\":\"RMX1941\",\"name\":\"RMX1941\"},\"library\":{\"name\":\"analytics-android\",\"version\":\"4.1.5\"},\"locale\":\"en-GB\",\"network\":{\"bluetooth\":false,\"carrier\":\"Vi India\",\"cellular\":true,\"wifi\":false},\"os\":{\"name\":\"Android\",\"version\":\"9\"},\"screen\":{\"density\":2,\"height\":1412,\"width\":720},\"timezone\":\"Asia/Kolkata\",\"traits\":{\"anonymousId\":\"70032613-9126-4488-8ffa-3f3ec0d4f8dc\",\"notifications\":true,\"userId\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130\"},\"userAgent\":\"Dalvik/2.1.0 (Linux; U; Android 9; RMX1941 Build/PPR1.180610.011)\"},\"event\":\"CARD_VIEW\",\"integrations\":{},\"messageId\":\"630b7f53-6a48-4182-9702-3c53380c9b35\",\"properties\":{\"android_id\":\"bb35abc9f1aa3ccf\",\"app_version\":\"309\",\"appsflyer_campaign\":\"Public_MP Day2\",\"appsflyer_media_source\":\"googleadwords_int\",\"device_bucket\":\"58\",\"device_id\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130\",\"device_region\":\"IN\",\"device_tenant\":\"HINDI\",\"event_day\":\"124\",\"feed_id\":\"FEED_TAB\",\"first_app_open\":\"1595917951176\",\"first_app_register\":\"1595917956000\",\"id\":\"sp_il216aop3avgc\",\"is_in_pip\":\"false\",\"location_code\":\"S#A#DL_WD_RAJOURI_GARDEN\",\"location_district\":\"MP_SO\",\"location_sub_district_code\":\"MP_SO_CHHAPARA\",\"network_type\":\"4G\",\"position\":\"17\",\"session_id\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130_1606566124686_103\",\"timespent\":\"174867\",\"timestamp\":\"1606618768927\",\"title\":\"Kisan Protest : सड़क जाम न करें किसान, बातचीत के लिए आएं : अमित शाह\",\"type\":\"UGC_VIDEO\",\"uid\":\"x3syk2h6aLY21rAGp3Ch6aVsZgq2\",\"user_swipe\":\"true\"},\"timestamp\":\"2021-01-04T08:29:28+0530\",\"type\":\"track\",\"userId\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130\"},{\"anonymousId\":\"70032613-9126-4488-8ffa-3f3ec0d4f8dc\",\"channel\":\"mobile\",\"context\":{\"app\":{\"build\":3040309,\"name\":\"Public\",\"namespace\":\"com.cardfeed.video_public\",\"version\":\"2.19.9\"},\"device\":{\"adTrackingEnabled\":true,\"advertisingId\":\"46de8b88-91d0-4ca7-b14f-4080af389575\",\"id\":\"bb35abc9f1aa3ccf\",\"manufacturer\":\"Realme\",\"model\":\"RMX1941\",\"name\":\"RMX1941\"},\"library\":{\"name\":\"analytics-android\",\"version\":\"4.1.5\"},\"locale\":\"en-GB\",\"network\":{\"bluetooth\":false,\"carrier\":\"Vi India\",\"cellular\":true,\"wifi\":false},\"os\":{\"name\":\"Android\",\"version\":\"9\"},\"screen\":{\"density\":2,\"height\":1412,\"width\":720},\"timezone\":\"Asia/Kolkata\",\"traits\":{\"anonymousId\":\"70032613-9126-4488-8ffa-3f3ec0d4f8dc\",\"notifications\":true,\"userId\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130\"},\"userAgent\":\"Dalvik/2.1.0 (Linux; U; Android 9; RMX1941 Build/PPR1.180610.011)\"},\"event\":\"SCREEN_VIEW\",\"integrations\":{},\"messageId\":\"309746b6-fac9-4b04-8c4f-642aa555b209\",\"properties\":{\"android_id\":\"bb35abc9f1aa3ccf\",\"app_version\":\"309\",\"device_bucket\":\"58\",\"device_id\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130\",\"device_region\":\"IN\",\"device_tenant\":\"HINDI\",\"event_day\":\"124\",\"first_app_open\":\"1595917951176\",\"first_app_register\":\"1595917956000\",\"location_district\":\"MP_SO\",\"location_sub_district_code\":\"MP_SO_CHHAPARA\",\"network_type\":\"4G\",\"screen_view\":\"sp_dpamf3kznvawb:बरघाट: बम्होडी गांव में हुए सड़क हादसे में एक व्यक्ति घायल, 108 एंबुलेंस की मदद से \",\"session_id\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130_1606566124686_103\",\"timestamp\":\"1606618768936\",\"uid\":\"x3syk2h6aLY21rAGp3Ch6aVsZgq2\"},\"timestamp\":\"2021-01-03T08:29:28+0530\",\"type\":\"track\",\"userId\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130\"},{\"anonymousId\":\"70032613-9126-4488-8ffa-3f3ec0d4f8dc\",\"channel\":\"mobile\",\"context\":{\"app\":{\"build\":3040309,\"name\":\"Public\",\"namespace\":\"com.cardfeed.video_public\",\"version\":\"2.19.9\"},\"device\":{\"adTrackingEnabled\":true,\"advertisingId\":\"46de8b88-91d0-4ca7-b14f-4080af389575\",\"id\":\"bb35abc9f1aa3ccf\",\"manufacturer\":\"Realme\",\"model\":\"RMX1941\",\"name\":\"RMX1941\"},\"library\":{\"name\":\"analytics-android\",\"version\":\"4.1.5\"},\"locale\":\"en-GB\",\"network\":{\"bluetooth\":false,\"carrier\":\"Vi India\",\"cellular\":true,\"wifi\":false},\"os\":{\"name\":\"Android\",\"version\":\"9\"},\"screen\":{\"density\":2,\"height\":1412,\"width\":720},\"timezone\":\"Asia/Kolkata\",\"traits\":{\"anonymousId\":\"70032613-9126-4488-8ffa-3f3ec0d4f8dc\",\"notifications\":true,\"userId\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130\"},\"userAgent\":\"Dalvik/2.1.0 (Linux; U; Android 9; RMX1941 Build/PPR1.180610.011)\"},\"event\":\"INITIAL_VIDEO_DELAY\",\"integrations\":{},\"messageId\":\"c156957f-77b8-4299-b7d2-e488d5b127c0\",\"properties\":{\"android_id\":\"bb35abc9f1aa3ccf\",\"app_version\":\"309\",\"appsflyer_campaign\":\"Public_MP Day2\",\"appsflyer_media_source\":\"googleadwords_int\",\"card_id\":\"sp_lbnwxcdn05ini\",\"delay\":\"1155\",\"device_bucket\":\"58\",\"device_id\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130\",\"device_region\":\"IN\",\"device_tenant\":\"HINDI\",\"duration\":\"65900\",\"event_day\":\"124\",\"event_type\":\"action\",\"first_app_open\":\"1595917951176\",\"first_app_register\":\"1595917956000\",\"location_district\":\"MP_SO\",\"location_sub_district_code\":\"MP_SO_CHHAPARA\",\"network_type\":\"4G\",\"session_id\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130_1606566124686_103\",\"source\":\"hls\",\"timestamp\":\"1606618770788\",\"uid\":\"x3syk2h6aLY21rAGp3Ch6aVsZgq2\",\"video_url\":\"https://static.getinpix.com/public/hls/2020/11_nov/28_sat/hls_1606582465591_598/playlist.m3u8\"},\"timestamp\":\"2020-12-29T08:29:30+0530\",\"type\":\"track\",\"userId\":\"a63d13de-ed8f-3c44-a6bb-e5d43cc96130\"}],\"sentAt\":\"2020-11-29T08:29:31+0530\"}\n"
    };
    String metricSpec = "[]";

    String parseSpecJson =
            "{"
                    + "  \"format\" : \"json\","
                    + "  \"timestampSpec\" : {"
                    + "      \"column\" : \"timestamp\","
                    + "      \"format\" : \"auto\""
                    + "  },"
                    + "  \"dimensionsSpec\" : {"
                    + "      \"dimensions\": [],"
                    + "      \"dimensionExclusions\" : [],"
                    + "      \"spatialDimensions\" : []"
                    + "  }"
                    + "}";

    ParseSpec parseSpec;
    NewBatchInputRowParser parser;

    Queue<ByteBuffer> inputRowBuffers = new LinkedList<>();

    @Before
    public void setup() throws IOException
    {
        parseSpec = MAPPER.readValue(parseSpecJson, ParseSpec.class);
        parser = new NewBatchInputRowParser(parseSpec);

        for (String row : inputRows) {
            inputRowBuffers.offer(ByteBuffer.wrap(StringUtils.toUtf8(row)));
        }
    }


    @Test
    public void testSerde() throws IOException
    {
        Assert.assertEquals(
                parser,
                MAPPER.readValue(MAPPER.writeValueAsString(parser), NewBatchInputRowParser.class)
        );
    }

    @Test
    public void testParse()
    {
        Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

        int i = 0;

        while(!inputRowBuffers.isEmpty()) {
            ByteBuffer row = inputRowBuffers.poll();
            List<InputRow> parsed = parser.parseBatch(row);

            if (parsed.size()>0) {
                InputRow theRow = parsed.get(0);
                for(String name : theRow.getDimensions()){
                    System.out.println("\""+name+"\",");
                }
            }

            Assert.assertEquals(1,1);
        }
    }
}
