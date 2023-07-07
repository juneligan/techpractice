package com.example.techpractice;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestUtil {
    public static List<Message> createMessages(int size, int channelSize) {
        return IntStream.range(0, size)
                .mapToObj(index -> {
                    int channelId = (index % channelSize) + 1;
                    return new Message(index + 1, channelId, "Message " + index);
                }).collect(Collectors.toList());
    }
}
