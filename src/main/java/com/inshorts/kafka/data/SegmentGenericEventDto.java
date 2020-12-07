package com.inshorts.kafka.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentGenericEventDto implements Serializable {

    private String anonymousId;
    private String event;
    private String timestamp;
    private Map<String, Object> properties = new HashMap<>();
    private ContextDto context;

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class ContextDto implements Serializable {
        //private ScreenDto screen;
        private DeviceDto device;
        private OSDto os;
        private AppDto app;
        private LibraryDto library;
        private String locale;
        private String timezone;
        private String ip;
        private NetworkDto network;
        private TraitsDto traits;
    }

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class DeviceDto implements Serializable {
        private String manufacturer;
        private String model;
        private String id; // androidid/ios-device-id
        private String advertisingId;
        private String type; //Device type
        private Boolean adTrackingEnabled = Boolean.FALSE;
    }

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class TraitsDto implements Serializable {
        private String language;
    }

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class NetworkDto implements Serializable {
        private String carrier;
        private Boolean bluetooth = Boolean.FALSE;
        private Boolean cellular = Boolean.FALSE;
        private Boolean wifi = Boolean.FALSE;
    }

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class LibraryDto implements Serializable {
        private String name;
        private String version;
    }

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class OSDto implements Serializable {
        private String name;
        private String version;
    }

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class AppDto implements Serializable {
        private String name;
        private String version;
        private String namespace;
    }

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class ScreenDto implements Serializable {
        private Integer width = 0;
        private Integer height = 0;
    }

}



