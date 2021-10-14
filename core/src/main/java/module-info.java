module morphia.core {
    requires org.mongodb.bson;
    requires org.mongodb.driver.core;
    requires org.mongodb.driver.sync.client;
    requires io.github.classgraph;
    requires org.slf4j;
    requires java.desktop;
    requires net.bytebuddy;
    requires com.github.spotbugs.annotations;

    exports dev.morphia;
    exports dev.morphia.annotations;
    exports dev.morphia.mapping.codec;
    exports dev.morphia.mapping.codec.pojo;
    exports dev.morphia.mapping.conventions;
}