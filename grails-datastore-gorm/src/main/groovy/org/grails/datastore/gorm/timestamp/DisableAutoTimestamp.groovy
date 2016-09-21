package org.grails.datastore.gorm.timestamp

import groovy.transform.CompileStatic
import org.grails.datastore.gorm.events.AutoTimestampEventListener
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.support.AbstractApplicationContext

/**
 * Defines methods to temporarily disable auto timestamp events for domain classes
 *
 * @author Graeme Rocher
 * @since 6.0
 */
@CompileStatic
trait DisableAutoTimestamp {

    @Autowired(required = false)
    ApplicationContext applicationContext

    private AutoTimestampEventListener autoTimestampEventListener

    private AutoTimestampEventListener getListener() {
        if (autoTimestampEventListener == null) {
            def configurableApplicationContext = (AbstractApplicationContext) applicationContext
            autoTimestampEventListener = (AutoTimestampEventListener)configurableApplicationContext.applicationListeners.find { it.class == AutoTimestampEventListener }
        }
        autoTimestampEventListener
    }

    void withoutLastUpdated(Closure callable) {
        AutoTimestampEventListener listener = getListener()
        Map originalValues = [:]
        listener.entitiesWithLastUpdated.each {
            originalValues[it.key] = it.value
            it.value = false
        }
        callable.call()
        listener.entitiesWithLastUpdated.each {
            it.value = originalValues[it.key]
        }
    }

    void withoutLastUpdated(Class clazz, Closure callable) {
        AutoTimestampEventListener listener = getListener()
        Boolean originalValue = listener.entitiesWithLastUpdated[clazz.name]
        listener.entitiesWithLastUpdated[clazz.name] = false
        callable.call()
        listener.entitiesWithLastUpdated[clazz.name] = originalValue
    }

    void withoutDateCreated(Closure callable) {
        AutoTimestampEventListener listener = getListener()
        Map originalValues = [:]
        listener.entitiesWithDateCreated.each {
            originalValues[it.key] = it.value
            it.value = false
        }
        callable.call()
        listener.entitiesWithDateCreated.each {
            it.value = originalValues[it.key]
        }
    }

    void withoutDateCreated(Class clazz, Closure callable) {
        AutoTimestampEventListener listener = getListener()
        Boolean originalValue = listener.entitiesWithDateCreated[clazz.name]
        listener.entitiesWithDateCreated[clazz.name] = false
        callable.call()
        listener.entitiesWithDateCreated[clazz.name] = originalValue
    }

    void withoutAutoTimestamp(Closure callable) {
        withoutLastUpdated {
            withoutDateCreated(callable)
        }
    }

    void withoutAutoTimestamp(Class clazz, Closure callable) {
        withoutLastUpdated(clazz) {
            withoutDateCreated(clazz, callable)
        }
    }
}