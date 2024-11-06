package fuchs.ad_producer.model;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class ClickEventFactory {

    private static final Random random = new Random();

    private static final List<String> LOCATIONS = Arrays.asList(
            "New York","New York","New York", "Los Angeles","Los Angeles", "Chicago",
            "Chicago", "Houston", "Phoenix", "Philadelphia", "San Francisco",
            "San Francisco","San Francisco", "San Diego", "Dallas", "San Jose"
    );

    private static final List<String> DEVICE_TYPES = Arrays.asList(
            "mobile","mobile","mobile", "desktop"
    );

    private static final List<String> CATEGORIES = Arrays.asList(
            "electronics", "electronics", "fashion", "fashion", "sports", "home", "home","beauty","beauty", "toys", "automotive", "books"
    );

    private static final List<String> PLATFORMS = Arrays.asList(
            "Google Ads","Google Ads", "Facebook","Facebook","Facebook", "Instagram",
            "Instagram","Instagram","Instagram","YouTube"
    );

    private static final Map<String, String> CATEGORY_BY_LOCATION = Map.of(
            "New York", "electronics",
            "Los Angeles", "fashion",
            "Chicago", "home",
            "Houston", "beauty",
            "Phoenix", "sports",
            "Philadelphia", "home",
            "San Francisco", "electronics",
            "San Diego", "electronics",
            "Dallas", "automotive",
            "San Jose", "home"
    );

    private static final Map<String, String> CATEGORY_BY_PLATFORM = Map.of(
            "Google Ads", "electronics",
            "Facebook", "home",
            "Instagram", "beauty",
            "YouTube", "toys"
    );


    public static ClickEvent generateRandomClickEvent() {
        String eventId = UUID.randomUUID().toString();
        String userId = String.valueOf(random.nextInt(1000000));
        String adId = String.valueOf(random.nextInt(20000));
        String location = LOCATIONS.get(random.nextInt(LOCATIONS.size()));
        long timestamp = generateRandomPastTimestamp(30, location);
        String deviceType = DEVICE_TYPES.get(random.nextInt(DEVICE_TYPES.size()));
        String platform = PLATFORMS.get(random.nextInt(PLATFORMS.size()));

        String category = generateRandomCategory(location,platform);

        return new ClickEvent(eventId, timestamp, userId, adId, location, deviceType, category, platform);
    }


    public static String generateRandomCategory(String location, String platform){
        if(random.nextDouble()<0.5){
            if(random.nextDouble()<0.6) return CATEGORY_BY_LOCATION.get(location);
            return CATEGORY_BY_PLATFORM.get(platform);
        }
        return CATEGORIES.get(random.nextInt(CATEGORIES.size()));
    }

    private static final Map<String, ZoneId> LOCATION_TIMEZONES = Map.of(
            "New York", ZoneId.of("America/New_York"),
            "Los Angeles", ZoneId.of("America/Los_Angeles"),
            "Chicago", ZoneId.of("America/Chicago"),
            "Houston", ZoneId.of("America/Chicago"),
            "Phoenix", ZoneId.of("America/Phoenix"),
            "Philadelphia", ZoneId.of("America/New_York"),
            "San Francisco", ZoneId.of("America/Los_Angeles"),
            "San Diego", ZoneId.of("America/Los_Angeles"),
            "Dallas", ZoneId.of("America/Chicago"),
            "San Jose", ZoneId.of("America/Los_Angeles")
    );

    public static long generateRandomPastTimestamp(int daysBack, String location) {
        long now = Instant.now().toEpochMilli();
        long past = Instant.now().minusSeconds(daysBack * 24L * 60 * 60).toEpochMilli();

        ZoneId zoneId = LOCATION_TIMEZONES.getOrDefault(location, ZoneId.of("UTC"));


        LocalDateTime peakTime = LocalDateTime.now(zoneId)
                .minusDays(random.nextInt(daysBack))
                .withHour(18 + random.nextInt(3))
                .withMinute(random.nextInt(60))
                .withSecond(random.nextInt(60));

        long peakTimestamp = peakTime.atZone(zoneId).toInstant().toEpochMilli();


        return random.nextDouble() < 0.5 ? peakTimestamp : past + (long) (random.nextDouble() * (now - past));
    }
}
