package com.inshorts.kafka.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class VideoDataSourceFlattennedDto implements Serializable {


    //Common
    private String eventSource;
    private String categoryWhenEventHappened;
    private Integer ads = -1;
    private String sessionIds;
    private Integer sessionNthValue = -1;
    private String advertisingId;
    private String osVersion;
    private String appVersion;
    private String deviceCountry;
    private Long birthDate;
    private Integer deviceNew = -1;
    private String googleAdvertisingId;
    private Integer jailbroken = 0;
    private String tenant;
    private String model;
    private String carrier;
    private String networkType;
    private String platform;
    private String acquireSource;
    private String acquireCampaign;
    private String dataConn;
    private String district;
    private String latitude;
    private String longitude;
    private String pinCode;
    private String subDistrict;
    private String districtCode;
    private String subDistrictCode;

    private String locationCode;

    //Event data
    private String eventName;
    private String eventId;
    private Long eventTimeStamp;
    private String timestamp;
    private String eventDateUTC;
    private Integer eventDay = 0;


    //Device data
    private String deviceId;
    private String uniqueId;
    private Long cardCount;
    private Long sessionCount;
    private Long appOpenCount;
    private Long firstAppOpen;
    private Long firstAppRegister;
    private Long LastAppOpen;
    private Integer isLoggedIn;
    private Integer customDeviceNew;
    private List<String> deviceTypeCategories;
    private String networkQuality;
    private Double networkSpeed; //Kbit/seconds
    //NewFieldsAdded
    //27March2018
    private String userId;


    //App Open/Closed/Resume
    private String activityName;

    //Follow
    private String followLabel;
    private Integer followed;

    //Other Event
    private String readExternalStoragePermission;
    private String failureType;
    private String loginLabel;

    //Event flags
    private Integer appOpen;
    private Integer followClick;


    //Phone number verficiation fields
    private String verifier;
    private String error;

    private String permission;
    private String permissionValue;

    private String label;
    private String value;


    //Card Data
    private String cardId;
    private String title;
    private String cardType;

    //Card View and timeSpent data
    private String customEvent1;
    private String customEvent2;
    private String customEvent3;
    private Long timeSpent;
    private Long position;
    private String feedId;
    private String sliderId;
    private Integer sliderPosition;
    private String pollId;
    private String videoId;
    private Integer focusedPosition;
    private String playerName;

    //Full Story Open Events
    private String fullStoryType;
    private String fullStorySubtype;

    //Share data
    private String sharedOn;

    //Other Event
    private String appState;
    private Integer notificationSession = 0;

    //Event flags
    private Integer cardView = 0;
    private Integer fullStoryView = 0;
    private Integer fullStoryOpen = 0;
    private Integer youTubeView = 0;
    private Integer notificationOpen = 0;
    private Integer notificationReceived = 0;
    private Integer notificationShown = 0;
    private Integer shareClick = 0;
    private Integer shareEvent = 0;
    private Integer shareSuccess = 0;
    private Integer deepLinkOpen = 0;
    private Integer pollAnswered = 0;
    private String notificationSource;
    private String notificationType;

    //NewFieldsAdded
    //10Nov2017
    private String deviceTenant;
    private String cardTenant;

    //NewEventAddedPollAnswered
    //9Apr2017
    private String pollAnswer;
    private String source;

    private String region;
    private String action;

    private String type;

    // Custom Card Events
    private Integer verticalPosition;
    private Integer horizontalPosition;
    private String ccType;
    private String adCampaignName;
    private Integer cardClick = 0;
    private Double cardTimeSpent = 0.0;
    private Integer cardVideoClick = 0;
    private Integer cardShare = 0;
    private Integer cardOpenUrl = 0;
    private Integer cardMarkedForDeletion = 0;
    private Boolean valid;
    private Integer swipeLeft = 0;
    private Integer swipeRight = 0;

    private String tag;
    private String selectedOption;

    //Slider Card Fields
    private String isNative;
    private String actionId;

}

