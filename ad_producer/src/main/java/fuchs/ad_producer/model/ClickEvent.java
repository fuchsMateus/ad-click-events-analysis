package fuchs.ad_producer.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClickEvent {
    private String eventId;
    private long timestamp;
    private String userId;
    private String adId;
    private String location;
    private String deviceType;
    private String category;
    private String platform;
}

