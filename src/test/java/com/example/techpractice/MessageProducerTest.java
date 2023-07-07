package com.example.techpractice;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static com.example.techpractice.TestUtil.createMessages;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageProducerTest {

    @Mock
    private ChannelListener mockedChannelListener;

    @Test
    void runTest() {
        // given
        List<Message> messages = createMessages(15, 4);
        MessageProducer messageProducer = new MessageProducer(mockedChannelListener, messages);

        doNothing().when(mockedChannelListener).onMessageReceived(any(Message.class));

        // when
        messageProducer.run();

        // then
        verify(mockedChannelListener, times(15)).onMessageReceived(any(Message.class));
    }
}