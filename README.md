
1. Go to your terminal
2. Go to the project directory
3. Execute this to run the test
```shell
 ./gradlew test --tests com.example.techpractice.OrderingModuleTest
 ./gradlew test --tests com.example.techpractice.MessageConsumerTest
 ./gradlew test --tests com.example.techpractice.MessageProducerTest
```

4. Execute this to run the app
```shell
./gradlew bootRun --args='--message.size=10 --output.size=6'
```
 - 4.1 NOTE: you can add args for `message.size` and `output.size` and its values,
 - If not present, it will just use the default value which 505 and 500 respectively
 - The default value is for my own testing only, no solid reasons :D
