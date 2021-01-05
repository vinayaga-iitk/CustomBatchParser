package com.inshorts.kafka.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.inshorts.kafka.data.KafkaValueDeserializer;
import com.inshorts.kafka.data.SegmentBatchGenericEventDto;
import com.inshorts.kafka.data.SegmentGenericEventDto;
import com.inshorts.kafka.data.VideoDataSourceFlattennedDto;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.Parser;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.IOException;
import java.util.logging.Logger;

public class NewBatchInputRowParser implements ByteBufferInputRowParser {
    public static final String TYPE_NAME = "customParser";
    public static final String UNKNOWN_STRING = "Unknown";
    private static final DateTimeFormatter bucketFmtPrefix = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(NewBatchInputRowParser.class);

    private final ParseSpec parseSpec;
    private final MapInputRowParser mapParser;
    private KafkaValueDeserializer kafkaValueDeserializer;
    private Gson gson;

    private Parser<String, Object> parser;

    @JsonCreator
    public NewBatchInputRowParser(
            @JsonProperty("parseSpec") ParseSpec parseSpec
    ) {
        this.parseSpec = Preconditions.checkNotNull(parseSpec, "parseSpec");
        this.mapParser = new MapInputRowParser(parseSpec);
        this.kafkaValueDeserializer = new KafkaValueDeserializer();
        this.gson = new Gson();
    }

    @Override
    public List<InputRow> parseBatch(ByteBuffer input) {
        try {
            FileWriter myWriter = new FileWriter("/Users/vinaykumaragarwal/Desktop/file1.txt");
            myWriter.write("ENTERING BATCH PARSER");
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        List<InputRow> finalEventList = new ArrayList<>();
        SegmentBatchGenericEventDto batchKafkaEvent = kafkaValueDeserializer.deserialize("", input.array());
//        String batchSerializedObj = gson.toJson(batchKafkaEvent);
//        Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
//        logger.info(batchSerializedObj);
        for (SegmentGenericEventDto segmentGenericEventDto : batchKafkaEvent.getBatch()) {
            try {
                VideoDataSourceFlattennedDto druidNewsDataSourceEventDto = convertToFlattenDto(segmentGenericEventDto, new DateTime());
                if (druidNewsDataSourceEventDto == null) {
                    System.out.println("Found NULL");
                    continue;
                }
                String serializedObj = gson.toJson(druidNewsDataSourceEventDto);
//                Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
//                logger.info(serializedObj);
                InputRow row = parse(serializedObj);
                finalEventList.add(row);
                System.out.println("Success");
            } catch (Exception e) {
                System.out.println("Failure");
                e.printStackTrace();
            }
        }
        return finalEventList;
    }

    @JsonProperty
    @Override
    public ParseSpec getParseSpec() {
        return parseSpec;
    }

    @Override
    public ByteBufferInputRowParser withParseSpec(ParseSpec parseSpec) {
        return new NewBatchInputRowParser(parseSpec);
    }

    public void initializeParser() {
        if (parser == null) {
            // parser should be created when it is really used to avoid unnecessary initialization of the underlying
            // parseSpec.
            parser = parseSpec.makeParser();
        }
    }

    public void startFileFromBeginning() {
        initializeParser();
        parser.startFileFromBeginning();
    }

    @Nullable
    public InputRow parse(@Nullable String input) {
        return parseMap(parseString(input));
    }

    @Nullable
    private Map<String, Object> parseString(@Nullable String inputString) {
        initializeParser();
        return parser.parseToMap(inputString);
    }

    @Nullable
    private InputRow parseMap(@Nullable Map<String, Object> theMap) {
        // If a header is present in the data (and with proper configurations), a null is returned
        if (theMap == null) {
            return null;
        }
        return Iterators.getOnlyElement(mapParser.parseBatch(theMap).iterator());
    }

    private static VideoDataSourceFlattennedDto convertToFlattenDto(SegmentGenericEventDto eventDto, DateTime finalStartDateTime) throws ParseException {
        if (eventDto.getEvent() == null) {
            return null;
        }
        DateTime eventTimestamp = null;
        DateTime currentTime = DateTime.now();
        VideoDataSourceFlattennedDto druidNewsDataSourceEventDto = new VideoDataSourceFlattennedDto();
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(eventDto.getTimestamp()) == true) {
            if (org.apache.commons.lang3.StringUtils.isNotEmpty(eventDto.getTimestamp().trim()) == true) {
                try {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
                    DateTime eventDateTime = new DateTime(format.parse(eventDto.getTimestamp().trim()));
                    druidNewsDataSourceEventDto.setEventTimeStamp(eventDateTime.getMillis());
                    eventTimestamp = eventDateTime;
                } catch (Exception e) {
                    try {
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssssZ");
                        DateTime eventDateTime = new DateTime(format.parse(eventDto.getTimestamp().trim()));
                        druidNewsDataSourceEventDto.setEventTimeStamp(eventDateTime.getMillis());
                        eventTimestamp = eventDateTime;
                    } catch (Exception ex) {
                        druidNewsDataSourceEventDto.setEventTimeStamp(finalStartDateTime.getMillis());
                    }
                }
            }
        } else {
            druidNewsDataSourceEventDto.setEventTimeStamp(finalStartDateTime.getMillis());
        }
        druidNewsDataSourceEventDto.setEventName(eventDto.getEvent());
        druidNewsDataSourceEventDto.setEventSource("SEGMENT-V2");
        if (eventTimestamp != null && (eventTimestamp.compareTo(currentTime.plusDays(2)) < 0 && eventTimestamp.compareTo(currentTime.plusDays(-7)) > 0)) {
            druidNewsDataSourceEventDto.setTimestamp(eventDto.getTimestamp());
        } else {
            return null;
        }

        if (eventDto.getContext() != null) {
            druidNewsDataSourceEventDto.setDeviceCountry(eventDto.getContext().getLocale());
            if (eventDto.getContext().getOs() != null) {
                if (org.apache.commons.lang3.StringUtils.isNotEmpty(eventDto.getContext().getOs().getName())) {
                    druidNewsDataSourceEventDto.setPlatform(eventDto.getContext().getOs().getName().toUpperCase());
                }
                druidNewsDataSourceEventDto.setOsVersion(eventDto.getContext().getOs().getVersion());
            }
            if (eventDto.getContext().getDevice() != null) {
                druidNewsDataSourceEventDto.setUniqueId(eventDto.getContext().getDevice().getId());
                druidNewsDataSourceEventDto.setAdvertisingId(eventDto.getContext().getDevice().getAdvertisingId());
                druidNewsDataSourceEventDto.setModel(eventDto.getContext().getDevice().getModel());
                if (eventDto.getContext().getDevice().getAdTrackingEnabled() != null && eventDto.getContext().getDevice().getAdTrackingEnabled()) {
                    druidNewsDataSourceEventDto.setAds(1);
                } else {
                    druidNewsDataSourceEventDto.setAds(-1);
                }
            }
            if (eventDto.getContext().getNetwork() != null) {
                druidNewsDataSourceEventDto.setCarrier(eventDto.getContext().getNetwork().getCarrier());
            }
            if (eventDto.getContext().getApp() != null) {
                druidNewsDataSourceEventDto.setAppVersion(eventDto.getContext().getApp().getVersion());
            }
        }

        Map<String, Object> properties = eventDto.getProperties();
        if (properties == null) {
            properties = new HashMap<>();
        }
        druidNewsDataSourceEventDto.setCategoryWhenEventHappened((String) properties.getOrDefault("category", UNKNOWN_STRING));
        druidNewsDataSourceEventDto.setSessionNthValue(Double.valueOf(String.valueOf(properties.getOrDefault("session_count", "-1.0"))).intValue());
        druidNewsDataSourceEventDto.setSessionIds((String) properties.getOrDefault("session_id", UNKNOWN_STRING));
        druidNewsDataSourceEventDto.setNetworkType((String) properties.getOrDefault("network_type", UNKNOWN_STRING));
        druidNewsDataSourceEventDto.setDeviceTenant(String.valueOf(properties.getOrDefault("device_tenant", "ENGLISH")).toUpperCase());
        druidNewsDataSourceEventDto.setNetworkQuality((String) properties.getOrDefault("network_quality", UNKNOWN_STRING));
        druidNewsDataSourceEventDto.setNetworkSpeed(Double.valueOf((String) properties.getOrDefault("network_speed", "0.0")));
        druidNewsDataSourceEventDto.setDeviceId((String) properties.getOrDefault("device_id", UNKNOWN_STRING));
        druidNewsDataSourceEventDto.setCardId((String) properties.getOrDefault("id", UNKNOWN_STRING));
        if (org.apache.commons.lang3.StringUtils.equalsIgnoreCase(druidNewsDataSourceEventDto.getCardId(), UNKNOWN_STRING)) {
            druidNewsDataSourceEventDto.setCardId((String) properties.getOrDefault("card_id", UNKNOWN_STRING));
        }

        if (StringUtils.isNotEmpty(druidNewsDataSourceEventDto.getCardId())) {
            String[] split = druidNewsDataSourceEventDto.getCardId().split("_");
            if (split.length >= 2) {
                druidNewsDataSourceEventDto.setCardId(split[0] + "_" + split[1]);
            }
        }
        druidNewsDataSourceEventDto.setEventDateUTC(bucketFmtPrefix.print(druidNewsDataSourceEventDto.getEventTimeStamp()));
        druidNewsDataSourceEventDto.setDistrict(String.valueOf(properties.getOrDefault("district_id", UNKNOWN_STRING)).toUpperCase());


        druidNewsDataSourceEventDto.setSubDistrictCode(String.valueOf(properties.getOrDefault("location_sub_district_code", UNKNOWN_STRING)).toUpperCase());
        druidNewsDataSourceEventDto.setSubDistrict(String.valueOf(properties.getOrDefault("location_subdistrict", UNKNOWN_STRING)).toUpperCase());
        druidNewsDataSourceEventDto.setDistrictCode(String.valueOf(properties.getOrDefault("location_district", UNKNOWN_STRING)).toUpperCase());
        druidNewsDataSourceEventDto.setPinCode(String.valueOf(properties.getOrDefault("location_postal_code", UNKNOWN_STRING)).toUpperCase());
        druidNewsDataSourceEventDto.setLatitude(String.valueOf(properties.getOrDefault("location_lat", UNKNOWN_STRING)).toUpperCase());
        druidNewsDataSourceEventDto.setLongitude(String.valueOf(properties.getOrDefault("location_long", UNKNOWN_STRING)).toUpperCase());
        druidNewsDataSourceEventDto.setSource(String.valueOf(properties.getOrDefault("source", UNKNOWN_STRING)).toUpperCase());
        druidNewsDataSourceEventDto.setLocationCode(String.valueOf(properties.getOrDefault("location_code", UNKNOWN_STRING)).toUpperCase());

        //Device Fields
        druidNewsDataSourceEventDto.setSessionCount(Long.valueOf((String) properties.getOrDefault("session_count", "0")));
        druidNewsDataSourceEventDto.setAppOpenCount(Long.valueOf(((String) properties.getOrDefault("app_open_count", "0"))));
        druidNewsDataSourceEventDto.setCardCount(Long.valueOf(((String) properties.getOrDefault("card_count", "0"))));
        druidNewsDataSourceEventDto.setLastAppOpen(Long.valueOf(((String) properties.getOrDefault("last_app_open", "0"))));
        druidNewsDataSourceEventDto.setFirstAppOpen(Long.valueOf(((String) properties.getOrDefault("first_app_open", "0"))));
        druidNewsDataSourceEventDto.setUserId((String) properties.getOrDefault("uid", ""));

        if (Boolean.valueOf(((String) properties.getOrDefault("is_logged_in", "false")))) {
            druidNewsDataSourceEventDto.setIsLoggedIn(1);
        } else {
            druidNewsDataSourceEventDto.setIsLoggedIn(0);
        }

        if (druidNewsDataSourceEventDto.getFirstAppOpen().intValue() != 0) {
            druidNewsDataSourceEventDto.setEventDay(Days.daysBetween(new DateTime(druidNewsDataSourceEventDto.getFirstAppOpen()).withTimeAtStartOfDay(), new DateTime(druidNewsDataSourceEventDto.getEventTimeStamp()).withTimeAtStartOfDay()).getDays());
            if (druidNewsDataSourceEventDto.getEventDay() <= 0) {
                druidNewsDataSourceEventDto.setCustomDeviceNew(1);
            } else {
                druidNewsDataSourceEventDto.setCustomDeviceNew(0);
            }
        } else {
            druidNewsDataSourceEventDto.setEventDay(-1);
            druidNewsDataSourceEventDto.setCustomDeviceNew(-1);
        }

        druidNewsDataSourceEventDto.setRegion(String.valueOf(properties.getOrDefault("device_region", UNKNOWN_STRING)).toUpperCase());

        if (properties.containsKey("ad_campaign_name")) {
            druidNewsDataSourceEventDto.setAdCampaignName((String) properties.getOrDefault("ad_campaign_name", UNKNOWN_STRING));
        } else {
            druidNewsDataSourceEventDto.setAdCampaignName((String) properties.getOrDefault("adCampaignName", UNKNOWN_STRING));
        }


        if (properties.containsKey("card_id")) {
            druidNewsDataSourceEventDto.setCardId((String) properties.getOrDefault("card_id", UNKNOWN_STRING));
        } else {
            druidNewsDataSourceEventDto.setCardId((String) properties.getOrDefault("cardId", UNKNOWN_STRING));
        }

        if (properties.containsKey("tag")) {
            druidNewsDataSourceEventDto.setTag((String) properties.getOrDefault("tag", UNKNOWN_STRING));
        } else {
            druidNewsDataSourceEventDto.setTag((String) properties.getOrDefault("tag", UNKNOWN_STRING));
        }

        if (properties.containsKey("selected_option")) {
            druidNewsDataSourceEventDto.setSelectedOption((String) properties.getOrDefault("selected_option", UNKNOWN_STRING));
        } else {
            druidNewsDataSourceEventDto.setSelectedOption((String) properties.getOrDefault("selected_option", UNKNOWN_STRING));
        }

        if (properties.containsKey("timespent")) {
            try {
                druidNewsDataSourceEventDto.setTimeSpent(Long.valueOf(String.valueOf(properties.getOrDefault("timespent", "0.0"))));
            } catch (Exception e) {
                druidNewsDataSourceEventDto.setTimeSpent(0L);
            }
        } else {
            try {
                druidNewsDataSourceEventDto.setTimeSpent(Long.valueOf(String.valueOf(properties.getOrDefault("timeSpent", "0.0"))));
            } catch (Exception e) {
                druidNewsDataSourceEventDto.setTimeSpent(0L);
            }
        }

        // Timespent condition filter
        String event = eventDto.getEvent();
        if (event.equals("CARD_VIEW")) {
            if (druidNewsDataSourceEventDto.getTimeSpent() > 86400) {
                System.out.printf("Timespent for CARD_VIEW exceeding limit %d\n", druidNewsDataSourceEventDto.getTimeSpent());
                return null;
            }
        } else {
            if (druidNewsDataSourceEventDto.getTimeSpent() > 3600) {
                System.out.printf("Timespent for %s exceeding limit %d\n", event, druidNewsDataSourceEventDto.getTimeSpent());
                return null;
            }
        }
        int position = Double.valueOf(String.valueOf(properties.getOrDefault("position", "-1"))).intValue();
        if(IsCustomCardEvent(event)){
            if (properties.containsKey("ad_unit")) {
                druidNewsDataSourceEventDto.setCardId((String.valueOf(properties.get("ad_unit"))));
            } else if (properties.containsKey("c_id")) {
                druidNewsDataSourceEventDto.setCardId((String.valueOf(properties.getOrDefault("c_id", UNKNOWN_STRING))));
            } else if (properties.containsKey("id")) {
                druidNewsDataSourceEventDto.setCardId((String.valueOf(properties.getOrDefault("id", UNKNOWN_STRING))));
            } else if (properties.containsKey("card_id")) {
                String cId = String.valueOf(properties.get("card_id"));
                if (!cId.startsWith("cc_")) {
                    cId = "cc_" + cId;
                }
                druidNewsDataSourceEventDto.setCardId(cId);
            } else {
                druidNewsDataSourceEventDto.setCardId(UNKNOWN_STRING);
            }
            int positionFromCardId = splitCardIdAndGetPosition(druidNewsDataSourceEventDto);
            druidNewsDataSourceEventDto.setVerticalPosition(positionFromCardId);
            druidNewsDataSourceEventDto.setCcType(CCType.CUSTOM_CARD.name());
            druidNewsDataSourceEventDto.setAdCampaignName(String.valueOf(properties.getOrDefault("campaign", UNKNOWN_STRING)));
        }
        //Event Fields
        switch (eventDto.getEvent()) {
            case "CARD_VIEW": {
                druidNewsDataSourceEventDto.setTimeSpent(Long.valueOf((String) properties.getOrDefault("timespent", "0")));
                druidNewsDataSourceEventDto.setPosition(Long.valueOf((String) properties.getOrDefault("position", "-1")));
                druidNewsDataSourceEventDto.setCustomEvent1((String) properties.getOrDefault("custom_event1", ""));
                druidNewsDataSourceEventDto.setCustomEvent2((String) properties.getOrDefault("custom_event2", ""));
                druidNewsDataSourceEventDto.setCustomEvent3((String) properties.getOrDefault("custom_event3", ""));
                druidNewsDataSourceEventDto.setFeedId((String) properties.getOrDefault("feed_id", ""));
                druidNewsDataSourceEventDto.setSliderId((String) properties.getOrDefault("slider_id", ""));
                druidNewsDataSourceEventDto.setSliderPosition(Integer.valueOf((String) properties.getOrDefault("slider_position", "-1")));
                druidNewsDataSourceEventDto.setPollId((String) properties.getOrDefault("poll_id", ""));
                druidNewsDataSourceEventDto.setVideoId((String) properties.getOrDefault("video_id", ""));
                druidNewsDataSourceEventDto.setPlayerName((String) properties.getOrDefault("player_name", ""));
                druidNewsDataSourceEventDto.setFocusedPosition(Integer.valueOf((String) properties.getOrDefault("focused_position", "-1")));
                druidNewsDataSourceEventDto.setTimeSpent(Long.valueOf((String) properties.getOrDefault("timespent", "0")));
                druidNewsDataSourceEventDto.setCardView(1);
                break;
            }
            case "INITIAL_VIDEO_DELAY": {
                druidNewsDataSourceEventDto.setTimeSpent(Long.valueOf((String) properties.getOrDefault("delay", "0")));
                if (druidNewsDataSourceEventDto.getTimeSpent() > 3600) {
                    System.out.printf("Timespent for INITIAL_VIDEO_DELAY exceeding limit %d\n", druidNewsDataSourceEventDto.getTimeSpent());
                    return null;
                }
                break;
            }
            case "FULL_STORY_VIEW": {
                druidNewsDataSourceEventDto.setTimeSpent(Long.valueOf((String) properties.getOrDefault("timespent", "0")));
                druidNewsDataSourceEventDto.setPosition(Long.valueOf((String) properties.getOrDefault("position", "-1")));
                druidNewsDataSourceEventDto.setCustomEvent1((String) properties.getOrDefault("custom_event1", ""));
                druidNewsDataSourceEventDto.setCustomEvent2((String) properties.getOrDefault("custom_event2", ""));
                druidNewsDataSourceEventDto.setCustomEvent3((String) properties.getOrDefault("custom_event3", ""));
                druidNewsDataSourceEventDto.setFullStoryView(1);
                break;
            }
            case "FULL_STORY_OPEN": {
                druidNewsDataSourceEventDto.setTimeSpent(Long.valueOf((String) properties.getOrDefault("timespent", "0")));
                druidNewsDataSourceEventDto.setPosition(Long.valueOf((String) properties.getOrDefault("position", "-1")));
                druidNewsDataSourceEventDto.setFullStoryType((String) properties.getOrDefault("full_story_type", ""));
                druidNewsDataSourceEventDto.setFullStorySubtype((String) properties.getOrDefault("full_story_subtype", ""));
                druidNewsDataSourceEventDto.setFullStoryOpen(1);
                break;
            }
            case "NOTIFICATION_OPEN": {
                druidNewsDataSourceEventDto.setAppState((String) properties.getOrDefault("app_state", ""));
                druidNewsDataSourceEventDto.setNotificationOpen(1);
                druidNewsDataSourceEventDto.setNotificationType(String.valueOf(properties.getOrDefault("event_name", "NORMAL")));
                break;
            }
            case "NOTIFICATION_RECEIVED": {
                druidNewsDataSourceEventDto.setNotificationReceived(1);
                druidNewsDataSourceEventDto.setNotificationSource((String) properties.getOrDefault("source", ""));
                druidNewsDataSourceEventDto.setNotificationType(String.valueOf(properties.getOrDefault("event_name", "NORMAL")));
                break;
            }
            case "CUSTOM_CARD_ACTION": {
                druidNewsDataSourceEventDto.setAction((String) properties.getOrDefault("action", ""));
                break;
            }
            case "NOTIFICATION_SHOWN": {
                druidNewsDataSourceEventDto.setNotificationShown(1);
                druidNewsDataSourceEventDto.setNotificationSource((String) properties.getOrDefault("source", ""));
                druidNewsDataSourceEventDto.setNotificationType(String.valueOf(properties.getOrDefault("event_name", "NORMAL")));
                break;
            }
            case "SHARE_EVENT": {
                druidNewsDataSourceEventDto.setPosition(Long.valueOf((String) properties.getOrDefault("position", "-1")));
                druidNewsDataSourceEventDto.setShareEvent(1);
                break;
            }
            case "SHARE_CLICK": {
                druidNewsDataSourceEventDto.setPosition(Long.valueOf((String) properties.getOrDefault("position", "-1")));
                druidNewsDataSourceEventDto.setShareClick(1);
                break;
            }
            case "SHARE_EVENT_SUCCESS": {
                druidNewsDataSourceEventDto.setSharedOn((String) properties.getOrDefault("shared_on", ""));
                druidNewsDataSourceEventDto.setPosition(Long.valueOf((String) properties.getOrDefault("position", "-1")));
                druidNewsDataSourceEventDto.setShareSuccess(1);
                break;
            }
            case "DEEP_LINK_OPEN": {
                druidNewsDataSourceEventDto.setAppState((String) properties.getOrDefault("app_state", ""));
                druidNewsDataSourceEventDto.setDeepLinkOpen(1);
                break;
            }
            case "YOUTUBE_VIDEO_VIEW": {
                druidNewsDataSourceEventDto.setTimeSpent(Long.valueOf((String) properties.getOrDefault("timespent", "0")));
                druidNewsDataSourceEventDto.setYouTubeView(1);
                break;
            }
            case "POLL_ANSWERED": {
                druidNewsDataSourceEventDto.setPollAnswer((String) properties.getOrDefault("pollanswer", ""));
                druidNewsDataSourceEventDto.setPollAnswered(1);
                break;
            }
            case "PLUS_BUTTON_CLICKED": {
                druidNewsDataSourceEventDto.setCardId((String) properties.getOrDefault("card_id", UNKNOWN_STRING));
                break;
            }
            case "ME_TAB_VIEW":
            case "LOGIN_SCREEN_VIEW":
            case "SETTINGS_TAB_VIEW":
            case "LOCATION_PERMISSION_VIEW":
            case "LOCATION_SELECTION_VIEW":
            case "LOCATION_CONFIRMATION_VIEW": {
                druidNewsDataSourceEventDto.setTimeSpent(Long.valueOf((String) properties.getOrDefault("timespent", "0")));
                break;
            }
            case "APP_OPEN": {
                druidNewsDataSourceEventDto.setActivityName((String) properties.getOrDefault("activity_name", ""));
                druidNewsDataSourceEventDto.setAppOpen(1);
                break;
            }
            case "APP_RESUME":
            case "APP_PAUSE":
            case "APP_CLOSE": {
                druidNewsDataSourceEventDto.setActivityName((String) properties.getOrDefault("activity_tag", ""));
                break;
            }
            case "FOLLOW_CLICKED": {
                druidNewsDataSourceEventDto.setFollowLabel((String) properties.getOrDefault("label", ""));
                if (Boolean.valueOf((String) properties.getOrDefault("followed", "false"))) {
                    druidNewsDataSourceEventDto.setFollowed(1);
                } else {
                    druidNewsDataSourceEventDto.setFollowed(-1);
                }
                druidNewsDataSourceEventDto.setFollowClick(1);
                break;
            }
            case "FOLLOW_USER": {
                druidNewsDataSourceEventDto.setFollowLabel((String) properties.getOrDefault("user_id", ""));
                if (Boolean.valueOf((String) properties.getOrDefault("is_following", "false"))) {
                    druidNewsDataSourceEventDto.setFollowed(1);
                } else {
                    druidNewsDataSourceEventDto.setFollowed(-1);
                }
                druidNewsDataSourceEventDto.setSource((String) properties.getOrDefault("screen", ""));
                break;
            }
            case "BUTTON_CLICKED": {
                druidNewsDataSourceEventDto.setLoginLabel((String) properties.getOrDefault("label", ""));
                break;
            }
            case "LOGIN_FAIL": {
                druidNewsDataSourceEventDto.setFailureType((String) properties.getOrDefault("failure_type", ""));
                break;
            }
            case "PERMISSION_EVENT": {
                if (properties.containsKey("read_external_storage")) {
                    druidNewsDataSourceEventDto.setPermission("read_external_storage");
                    druidNewsDataSourceEventDto.setPermissionValue((String) properties.getOrDefault("read_external_storage", ""));
                } else if (properties.containsKey("read_external_storage_permission")) {
                    druidNewsDataSourceEventDto.setPermission("read_external_storage_permission");
                    druidNewsDataSourceEventDto.setPermissionValue((String) properties.getOrDefault("read_external_storage_permission", ""));
                } else if (properties.containsKey("fine_location_permission")) {
                    druidNewsDataSourceEventDto.setPermission("fine_location_permission");
                    druidNewsDataSourceEventDto.setPermissionValue((String) properties.getOrDefault("fine_location_permission", ""));
                } else if (properties.containsKey("coarse_location_permission")) {
                    druidNewsDataSourceEventDto.setPermission("coarse_location_permission");
                    druidNewsDataSourceEventDto.setPermissionValue((String) properties.getOrDefault("coarse_location_permission", ""));
                } else if (properties.containsKey("write_external_storage_permission")) {
                    druidNewsDataSourceEventDto.setPermission("write_external_storage_permission");
                    druidNewsDataSourceEventDto.setPermissionValue((String) properties.getOrDefault("write_external_storage_permission", ""));
                }
                break;
            }
            case "LOGIN_SUCCESS":
            case "SCREENSHOT_CAPTURED":
            case "PHONE_NUMBER_VERIFICATION_DISABLED":
            case "LOCATION_UPDATED": {
                break;

            }
            case "PHONE_NUMBER_VERIFICATION_SUCCESS": {
                druidNewsDataSourceEventDto.setVerifier((String) properties.getOrDefault("verifier", ""));
                break;
            }
            case "PHONE_NUMBER_VERIFICATION_FAILED": {
                druidNewsDataSourceEventDto.setVerifier((String) properties.getOrDefault("verifier", ""));
                druidNewsDataSourceEventDto.setError((String) properties.getOrDefault("error", ""));
                break;
            }
            case "LOCATION_CLICKED": {
                druidNewsDataSourceEventDto.setLabel((String) properties.getOrDefault("label", ""));
                druidNewsDataSourceEventDto.setValue((String) properties.getOrDefault("value", ""));
                break;
            }

            case "Custom Card View": {
                druidNewsDataSourceEventDto.setCardView(1);
                if (properties != null && properties.containsKey("cardId"))
                    druidNewsDataSourceEventDto.setCardId((String) properties.get("cardId"));
                if (properties != null && properties.containsKey("type"))
                    druidNewsDataSourceEventDto.setType((String) properties.get("type"));
                if (properties != null && properties.containsKey("campaign"))
                    druidNewsDataSourceEventDto.setAdCampaignName((String) properties.get("campaign"));
                if (properties != null && properties.containsKey("position") && org.apache.commons.lang3.StringUtils.isNotEmpty(String.valueOf(properties.getOrDefault("position", "-1.0")))) {
                    try {
                        druidNewsDataSourceEventDto.setPosition(Double.valueOf(String.valueOf(properties.getOrDefault("position", "-1.0"))).longValue());
                    } catch (Exception e) {
                        druidNewsDataSourceEventDto.setPosition(-1L);
                    }
                }
                if (properties.containsKey("timeSpent")) {
                    Double timeSpentCard = 0.0;
                    String timeSpent = String.valueOf(properties.get("timeSpent"));
                    if (timeSpent.contains(",")) {
                        timeSpent = timeSpent.replace(",", ".");
                    }
                    if (timeSpent.matches("^([0-9]+)?(\\.[0-9]+)?$")) {
                        timeSpentCard = Double.valueOf(timeSpent);
                    } else {
                        timeSpentCard = 0.0;
                    }
                    Double checkTime = 300.0;
                    if (properties != null && properties.containsKey("type") && String.valueOf(properties.get("type")).contains("video")) {
                        checkTime = 1200.0;
                    }
                    if (timeSpentCard > checkTime)
                        timeSpentCard = checkTime;
                    if (timeSpentCard < 0)
                        timeSpentCard = 0.0;
                    druidNewsDataSourceEventDto.setTimeSpent(timeSpentCard.longValue());
                }
                break;
            }

            case "FULL_PAGE_AD_CLICK": {
                druidNewsDataSourceEventDto.setCcType(CCType.FULL_PAGE.name());
                druidNewsDataSourceEventDto.setCardView(0);
                druidNewsDataSourceEventDto.setCardClick(1);
                druidNewsDataSourceEventDto.setVerticalPosition(position);
                druidNewsDataSourceEventDto.setHorizontalPosition(-1);
                break;
            }
            case "FULL_PAGE_AD_VIEW": {
                druidNewsDataSourceEventDto.setCcType(CCType.FULL_PAGE.name());
                druidNewsDataSourceEventDto.setCardView(1);
                druidNewsDataSourceEventDto.setCardClick(0);
                druidNewsDataSourceEventDto.setVerticalPosition(position);
                druidNewsDataSourceEventDto.setHorizontalPosition(-1);
                break;
            }
            case "BOTTOM_BAR_AD_CLICK": {
                druidNewsDataSourceEventDto.setCcType(CCType.BOTTOM.name());
                druidNewsDataSourceEventDto.setCardView(0);
                druidNewsDataSourceEventDto.setCardClick(1);
                druidNewsDataSourceEventDto.setVerticalPosition(position);
                druidNewsDataSourceEventDto.setHorizontalPosition(-1);
                break;
            }
            case "BOTTOM_BAR_AD_SHOWN": {
                druidNewsDataSourceEventDto.setCcType(CCType.BOTTOM.name());
                druidNewsDataSourceEventDto.setCardView(1);
                druidNewsDataSourceEventDto.setCardClick(0);
                druidNewsDataSourceEventDto.setVerticalPosition(position);
                druidNewsDataSourceEventDto.setHorizontalPosition(-1);
                break;
            }

            case "ad_click":
            case "brand_ad_click": {
                druidNewsDataSourceEventDto.setCardClick(1);
                druidNewsDataSourceEventDto.setHorizontalPosition(position);
                break;
            }
            case "ad_video_play":
            case "brand_video_ad_click": {
                druidNewsDataSourceEventDto.setCardVideoClick(1);
                druidNewsDataSourceEventDto.setHorizontalPosition(position);
                break;
            }

            /**
             * Slider events are received from website, vertical position is always 0. Position received in these
             * case is the horizontal position. Vertical Position is set as default : -1
             */

            case "Slider Swipe Left": {
                druidNewsDataSourceEventDto.setCardId(getCardIdFromPropertiesAfterSplit(druidNewsDataSourceEventDto.getCardId()));
                druidNewsDataSourceEventDto.setSwipeLeft(1);
//                druidNewsDataSourceEventDto.getTotalUniqueSwipeLeft().add(deviceId);
                druidNewsDataSourceEventDto.setHorizontalPosition(position);
                break;
            }
            case "Slider Swipe Right": {
                druidNewsDataSourceEventDto.setCardId(getCardIdFromPropertiesAfterSplit(druidNewsDataSourceEventDto.getCardId()));
                druidNewsDataSourceEventDto.setSwipeRight(1);
//                druidNewsDataSourceEventDto.getTotalUniqueSwipeRight().add(deviceId);
                druidNewsDataSourceEventDto.setHorizontalPosition(position);
                break;
            }
            case "Slider Card View": {
                druidNewsDataSourceEventDto.setCardId(getCardIdFromPropertiesAfterSplit(druidNewsDataSourceEventDto.getCardId()));
                druidNewsDataSourceEventDto.setCardView(1);
//                druidNewsDataSourceEventDto.getTotalUniqueViews().add(deviceId);
                druidNewsDataSourceEventDto.setHorizontalPosition(position);

                if (properties.containsKey("timeSpent")) {
                    Double timeSpentCard = 0.0;
                    String timeSpent = String.valueOf(properties.get("timeSpent"));
                    if (timeSpent.contains(",")) {
                        timeSpent = timeSpent.replace(",", ".");
                    }
                    if (timeSpent.matches("^([0-9]+)?(\\.[0-9]+)?$")) {
                        timeSpentCard = Double.valueOf(timeSpent);
                    } else {
                        timeSpentCard = 0.0;
                    }
                    Double checkTime = 1200000.0;
                    if (timeSpentCard > checkTime)
                        timeSpentCard = checkTime;
                    if (timeSpentCard < 0)
                        timeSpentCard = 0.0;
                    druidNewsDataSourceEventDto.setCardTimeSpent(timeSpentCard * 1.0 / 1000);
                }
                break;
            }
            case "Slider Ad Click": {
                druidNewsDataSourceEventDto.setCardId(getCardIdFromPropertiesAfterSplit(druidNewsDataSourceEventDto.getCardId()));
                druidNewsDataSourceEventDto.setCardClick(1);
//                druidNewsDataSourceEventDto.getTotalUniqueClicks().add(deviceId);
                druidNewsDataSourceEventDto.setHorizontalPosition(position);
                break;
            }


        }

        return druidNewsDataSourceEventDto;
    }

    public enum CCType {
        BOTTOM,
        FULL_PAGE,
        CUSTOM_CARD
    }

    private static String getCardIdFromPropertiesAfterSplit(String cId) {
        if (StringUtils.isNotEmpty(cId)) {
            String[] split = cId.split("_");
            if (split.length >= 2) {
                return split[0] + "_" + split[1];
            }

        }
        return cId;
    }

    private static Boolean IsCustomCardEvent(String event){
        return StringUtils.equalsIgnoreCase(event, "CARD_VIEW")
                || StringUtils.equalsIgnoreCase(event, "ad_click")
                || StringUtils.equalsIgnoreCase(event, "brand_ad_click")
                || StringUtils.equalsIgnoreCase(event, "ad_video_play")
                || StringUtils.equalsIgnoreCase(event, "brand_video_ad_click")
                || StringUtils.equalsIgnoreCase(event, "Slider Swipe Left")
                || StringUtils.equalsIgnoreCase(event, "Slider Swipe Right")
                || StringUtils.equalsIgnoreCase(event, "Slider Card View")
                || StringUtils.equalsIgnoreCase(event, "Slider Ad Click")
                || StringUtils.equalsIgnoreCase(event, "FULL_PAGE_AD_CLICK")
                || StringUtils.equalsIgnoreCase(event, "FULL_PAGE_AD_VIEW")
                || StringUtils.equalsIgnoreCase(event, "BOTTOM_BAR_AD_CLICK")
                || StringUtils.equalsIgnoreCase(event, "BOTTOM_BAR_AD_SHOWN")
                ;
    }

    private static Integer splitCardIdAndGetPosition(VideoDataSourceFlattennedDto videoCustomCardEventDto) {
        String cId = videoCustomCardEventDto.getCardId();
        String positionPrefix = "po";
        Integer position = 10000;

        String[] splits = cId.split("_");
        for (String split : splits) {
            if (split.startsWith(positionPrefix) && !positionPrefix.equalsIgnoreCase(split)) {
                try {
                    position = Integer.valueOf(split.substring(2));
                } catch (NumberFormatException e) {
                    LOG.error("Error in parsing position string", e);
                }
            }
        }

        return position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NewBatchInputRowParser that = (NewBatchInputRowParser) o;
        return parseSpec.equals(that.parseSpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parseSpec);
    }

    @Override
    public String toString() {
        return "NewBatchInputRowParser{" +
                "parseSpec=" + parseSpec +
                '}';
    }

}
