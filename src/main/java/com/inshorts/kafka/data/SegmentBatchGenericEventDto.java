package com.inshorts.kafka.data;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentBatchGenericEventDto implements Serializable {

    private List<SegmentGenericEventDto> batch;
}

