package streaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        readFile("src/streaming/Activities.txt");
    }

    public static void readFile(String filename) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));

            List<MonitoredData> collectedList = reader.lines()
                    .map((line) -> getMonitoredData(line))
                    .collect(Collectors.toList());



            System.out.print("2nd problem: ");
            long days = getDaysOfMonitoring(collectedList);
            System.out.println(days + " days.");


            System.out.print("3rd problem: ");
            Map<String, Integer> activityCount = getCountByActivityType(collectedList);
            System.out.println(activityCount);


            System.out.print("4th problem: ");

            /**
             * We consider an activity taking place in a day if it started in one =>
             * create a set of days by taking the startTime's day from each activity
             * and call the actual method for the result
             */
            Map<Integer, Map<String, Integer>> activityCountPerDay =
                    getActivityCountPerDay(collectedList.stream()
                            .map(monitoredData -> monitoredData.getStartTime()
                                    .getDayOfYear()).collect(Collectors.toSet()), collectedList);
            for (Map.Entry entry : activityCountPerDay.entrySet()) {
                System.out.println("Day: " + entry.getKey() + " activities: " + entry.getValue());
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * For a given set of days, we construct a map containing the day as key (Integer.valueOf)
     * and a key-value pair as its value which contains the activity which was found for that day and
     * the number of times it occurred.
     *
     * @param days
     * @param collectedList
     * @return
     */
    private static Map<Integer, Map<String, Integer>> getActivityCountPerDay(Set<Integer> days, List<MonitoredData> collectedList) {
        return days.stream().collect(
                Collectors.toMap(
                        Integer::valueOf,
                        day -> getCountByActivityTypeAndDay(collectedList, day)));

    }

    /**
     * Helper method. We iterate over all entries in the list, filter out those that don't match the day
     * we're given and collect them by using the activityLabel as the key,
     * assigning a default "1" value - because it's the first time we encounter that
     * activity for the day - and use the sum as the merge function - anytime another entry should
     * be added to the same key, we add to its value
     *
     * @param collectedList
     * @param day
     * @return
     */
    private static Map<String, Integer> getCountByActivityTypeAndDay(List<MonitoredData> collectedList, Integer day) {
        return collectedList.stream()
                .filter((monitoredData -> monitoredData.getStartTime().getDayOfYear() == day))
                .collect(Collectors.toMap(
                        MonitoredData::getActivityLabel,
                        (monitoredData) -> 1,
                        Integer::sum
                ));
    }

    /**
     * We iterate over all entries and create a new map using the activity name as key and a default
     * "1" value (first time we encounter it). Since we'll most likely find more entries with the
     * same name, we supply a merge function used in collisions which merges the values by calling
     * {@link Integer#sum(int, int)}.
     *
     * @param collectedList
     * @return
     */
    private static Map<String, Integer> getCountByActivityType(List<MonitoredData> collectedList) {
        return collectedList.stream()
                .collect(Collectors.toMap(
                        MonitoredData::getActivityLabel,
                        (monitoredData) -> 1,
                        Integer::sum
                ));
    }


    private static long getDaysOfMonitoring(List<MonitoredData> collectedList) {
        LocalDateTime firstActivityTime = getFirstActivityTime(collectedList);
        LocalDateTime lastActivityTime = getLastActivityTime(collectedList);
        return Duration.between(firstActivityTime, lastActivityTime).toDays();
    }

    /**
     * We can iterate over all the data, sort it with an inline comparator and
     * get the first element. The structure can be replaced with lambda
     * expression and then further with stream().min(Comparator.comparing...) as the next
     * method.
     *
     * @param data
     * @return
     */
    private static LocalDateTime getFirstActivityTime(List<MonitoredData> data) {

        Optional<MonitoredData> optionalMonitoredData = data.stream().sorted(new Comparator<MonitoredData>() {
            @Override
            public int compare(MonitoredData o1, MonitoredData o2) {
                return o1.getStartTime().compareTo(o2.getStartTime());
            }
        }).findFirst();
        return optionalMonitoredData.get().getStartTime();
    }

    private static LocalDateTime getLastActivityTime(List<MonitoredData> data) {
        Optional<MonitoredData> first = data.stream().max(Comparator.comparing(MonitoredData::getEndTime));
        return first.get().getEndTime();
    }

    private static MonitoredData getMonitoredData(String line) {
        String[] tokens = line.split("\t");
        LocalDateTime startTime = LocalDateTime.parse(tokens[0].replace(" ", "T"), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        LocalDateTime endTime = LocalDateTime.parse(tokens[2].replace(" ", "T"), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        return new MonitoredData(startTime, endTime, tokens[4]);
    }

}