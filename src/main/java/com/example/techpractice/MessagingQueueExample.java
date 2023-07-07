package com.example.techpractice;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

interface ChannelListener {
    void onMessageReceived(Message message);
}

interface OrderedMessageQueueProvider {
    java.util.Queue<Message> getOutputQueue();
}

record Message(long messageId, int sourceChannelId, String payload) {

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Message{");
        sb.append("messageId=").append(messageId);
        sb.append(",sourceChannelId=").append(sourceChannelId);
        sb.append(",payload='").append(payload).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return messageId == message.messageId
            && sourceChannelId == message.sourceChannelId
            && Objects.equals(payload, message.payload);
    }
}

class OrderingModule implements ChannelListener, OrderedMessageQueueProvider, Runnable {
    private static final int HIGHEST_TRANSFER_SIZE = 5;
    private static final int SECOND_HIGHEST_TRANSFER_SIZE = 3;
    private static final int DEFAULT_TRANSFER_SIZE = 1;
    private static final int DEFAULT_DELAY_IN_MILLS = 2000;
    private final Map<Integer, ConcurrentLinkedQueue<Message>> channelQueues = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> channelTransferSizes = new HashMap<>();
    private final BlockingQueue<Message> outputQueue;
    private final Integer delayInMillis;

    private final boolean shouldRunIndefinitely;

    public OrderingModule(int outputQueueLimit, Integer delayInMillis, boolean shouldRunIndefinitely) {
        this.outputQueue = new LinkedBlockingQueue<>(outputQueueLimit);
        this.delayInMillis = Optional.ofNullable(delayInMillis).orElse(DEFAULT_DELAY_IN_MILLS);
        this.shouldRunIndefinitely = shouldRunIndefinitely;
        setChannelTransferSizes();
    }

    @Override
    public void run() {
        do {
            Util.delay(delayInMillis);

            while (hasAvailableSpace()) {
                List<Map.Entry<Integer, ConcurrentLinkedQueue<Message>>> sortedByPriorityChannel = getSortedByPriorityChannel();
                if (sortedByPriorityChannel.isEmpty()) {
                    break;
                }

                for (Map.Entry<Integer, ConcurrentLinkedQueue<Message>> o : sortedByPriorityChannel) {
                    Integer channelId = o.getKey();
                    ConcurrentLinkedQueue<Message> channelQueue = o.getValue();
                    int transferSize = channelTransferSizes.get(channelId);
                    int queuedMessagesCounter = 0;

                    while (queuedMessagesCounter < transferSize && !channelQueue.isEmpty() && hasAvailableSpace()) {
                        Message poll = channelQueue.poll();
                        outputQueue.offer(poll);
                        queuedMessagesCounter++;
                    }
                }
            }
        } while (shouldRunIndefinitely);
    }

    @Override
    public void onMessageReceived(Message message) {
        // Add message to the non-blocking channel queue
        channelQueues.computeIfAbsent(message.sourceChannelId(), o -> new ConcurrentLinkedQueue<>()).offer(message);
    }

    @Override
    public java.util.Queue<Message> getOutputQueue() {
        return outputQueue;
    }

    public int getChannelsSize() {
        return channelTransferSizes.keySet().size();
    }

    private void setChannelTransferSizes() {
        this.channelTransferSizes.put(1, DEFAULT_TRANSFER_SIZE);
        this.channelTransferSizes.put(2, DEFAULT_TRANSFER_SIZE);
        this.channelTransferSizes.put(3, SECOND_HIGHEST_TRANSFER_SIZE);
        this.channelTransferSizes.put(4, HIGHEST_TRANSFER_SIZE);
    }

    private boolean hasAvailableSpace() {
        return outputQueue.remainingCapacity() > 0;
    }

    private List<Map.Entry<Integer, ConcurrentLinkedQueue<Message>>> getSortedByPriorityChannel() {
        return channelQueues.entrySet()
                .stream()
                .filter(o -> !o.getValue().isEmpty())
                .sorted(Comparator.comparing(o -> channelTransferSizes.get(o.getKey()), Comparator.reverseOrder()))
                .toList();
    }
}

class MessageProducer implements Runnable {
    private final ChannelListener channelListener;
    private final List<Message> messages;

    public MessageProducer(ChannelListener channelListener, List<Message> messages) {
        this.channelListener = channelListener;
        this.messages = messages;
    }

    @Override
    public void run() {
        messages.stream()
                .peek(o -> System.out.println("Produced: " + o.payload()))
                .forEach(channelListener::onMessageReceived);
    }
}

class Util {

    /**
     * For testing only
     **/
    public static void delay(int millis) {
        try {
            Thread.sleep(millis); // Sleep for a while to allow channel queues to grow
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class MessageConsumer implements Runnable {

    private final OrderedMessageQueueProvider orderedMessageQueueProvider;


    MessageConsumer(OrderedMessageQueueProvider orderedMessageQueueProvider) {
        this.orderedMessageQueueProvider = orderedMessageQueueProvider;
    }


    @Override
    public void run() {
        java.util.Queue<Message> outputQueue = orderedMessageQueueProvider.getOutputQueue();

        Util.delay(1000);
        consumeThenPrint(outputQueue, "-----1st Batch DONE");

        Util.delay(1000);
        consumeThenPrint(outputQueue, "-----2nd Batch DONE");
    }

    private static void consumeThenPrint(Queue<Message> outputQueue, String batchDoneMessage) {
        int size = outputQueue.size();
        IntStream.range(0, size).forEach(index -> {
            Message message = outputQueue.poll();
            if (message != null) {
                System.out.println(
                        "Consumed: " + message.payload()
                                + " Channel " + message.sourceChannelId()
                                + " Counter " + (index + 1)
                );
            }
        });
        System.out.println(batchDoneMessage);
    }
}
@SpringBootApplication
public class MessagingQueueExample implements ApplicationRunner {

    @Value("${message.size}")
    private Integer messageSize;
    @Value("${output.size}")
    private Integer outputSize;
    private static final int NUMBER_CHANNELS = 4;
    private static final int CHANNEL_ID_ADDER = 1;
    private static final int DEFAULT_OUTPUT_QUEUE_CAPACITY = 500;
    private static final int DEFAULT_MESSAGES_SIZE = 505; // to test the limit
    public static void main(String[] args) {
        SpringApplication.run(MessagingQueueExample.class, args);
    }

    @Override
    public void run(ApplicationArguments args) {
        OrderingModule orderingModule = new OrderingModule(
            Optional.ofNullable(outputSize).orElse(DEFAULT_OUTPUT_QUEUE_CAPACITY),
            10,
            true
        );

        // Create and start the ordering module thread
        Thread orderingModuleThread = new Thread(orderingModule);
        orderingModuleThread.start();

        // Create and start the message producer thread
        Thread producerThread = new Thread(new MessageProducer(orderingModule, createMessages()));
        producerThread.start();

        // Simulate consuming messages from the output queue
        MessageConsumer messageConsumer = new MessageConsumer(orderingModule);
        messageConsumer.run();
    }

    private List<Message> createMessages() {
        return IntStream.range(0, Optional.ofNullable(messageSize).orElse(DEFAULT_MESSAGES_SIZE))
                .mapToObj(index -> {
                    int channelId = (index % NUMBER_CHANNELS) + CHANNEL_ID_ADDER;
                    return new Message(index, channelId, "Message " + index);
                }).collect(Collectors.toList());
    }
}
