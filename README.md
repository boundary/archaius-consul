# archaius-consul


## A [consul](https://www.consul.io/)-backed configuration source for [Archaius](https://github.com/Netflix/archaius)

### Basic Usage:

```java
// setup a consul client ( see https://github.com/Ecwid/consul-api for more info)
final ConsulClient client = new ConsulClient("localhost");

final String rootPath = "my-app/config"; // values will correspond to consul values stored at /v1/kv/my-app/config

// create and start the configuration source
final ConsulWatchedConfigurationSource configSource = new ConsulWatchedConfigurationSource(rootPath, client);
configSource.startAsync();

// register the configuration source with archaius

ConcurrentCompositeConfiguration finalConfig = new ConcurrentCompositeConfiguration();
finalConfig.addConfiguration(new DynamicWatchedConfiguration(configSource), "consul-dynamic");

// install / configure other archaius sources as needed

 ConfigurationManager.install(finalConfig);

```

### registering callbacks

To register a callback on the "example" property, an integer with default value of 2:

```java

final DynamicIntProperty example = DynamicPropertyFactory.getInstance().getIntProperty("example", 2);
example.addCallback(new Runnable() {
    @Override
    public void run() {
        System.out.println("example changed: " + example.get());
    }
});
```

Now you should be able to see watched callbacks in action by updating the corresponding configuration variable:

```bash
curl -X PUT -d `date +%s` localhost:8500/v1/kv/my-app/config/example
```
