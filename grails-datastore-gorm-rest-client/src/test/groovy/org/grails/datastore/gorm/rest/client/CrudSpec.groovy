package org.grails.datastore.gorm.rest.client

import grails.persistence.Entity
import org.codehaus.groovy.grails.commons.DefaultGrailsApplication
import org.codehaus.groovy.grails.web.converters.configuration.ConvertersConfigurationHolder
import org.codehaus.groovy.grails.web.converters.configuration.ConvertersConfigurationInitializer
import org.codehaus.groovy.grails.web.servlet.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.web.client.RestTemplate

import static org.springframework.test.web.client.match.MockRestRequestMatchers.content
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.http.HttpStatus

/**
 * @author Graeme Rocher
 */
class CrudSpec extends RestClientDatastoreSpec{

    void "Test the get method issues a GET request and binds the result to the entity"() {
        when:"An entity is retrieved with the get method"
            RestTemplate rt = Book.getRestBuilder().restTemplate
            final mockServer = MockRestServiceServer.createServer(rt)
            mockServer.expect(requestTo("http://localhost:8080/book/1"))
                      .andExpect(method(HttpMethod.GET))
                      .andExpect(header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON.toString()))
                      .andRespond(withSuccess('{"id":1, "title":"The Stand", "pages":200}', MediaType.APPLICATION_JSON))

            Book b = Book.get(1)

        then:"The number of books that exist is correct"
            b != null
            b.id == 1L
            b.title == "The Stand"
            b.pages == 200
            mockServer.verify()

    }

    void "Test the getAll method issues a GET request and binds the result to the entity"() {
        when:"An entity is retrieved with the get method"
            RestTemplate rt = Book.getRestBuilder().restTemplate
            final mockServer = MockRestServiceServer.createServer(rt)
            mockServer.expect(requestTo("http://localhost:8080/book/1"))
                    .andExpect(method(HttpMethod.GET))
                    .andExpect(header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON.toString()))
                    .andRespond(withSuccess('{"id":1, "title":"The Stand", "pages":200}', MediaType.APPLICATION_JSON))

            List<Book> books = Book.getAll([1])

        then:"The number of books that exist is correct"
            books
            books[0] != null
            books[0].id == 1L
            books[0].title == "The Stand"
            books[0].pages == 200
            mockServer.verify()

    }

    void "Test the saveAll method issues a POST request with the correct data"() {
        when:"A new entity is saved"
            RestTemplate rt = Book.getRestBuilder().restTemplate
            final mockServer = MockRestServiceServer.createServer(rt)
            mockServer.expect(requestTo("http://localhost:8080/book"))
                    .andExpect(method(HttpMethod.POST))
                    .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                    .andExpect(content().string('{"class":"org.grails.datastore.gorm.rest.client.Book","id":null,"pages":null,"title":"The Stand"}'))
                    .andRespond(withSuccess('{"id":1, "title":"The Stand"}', MediaType.APPLICATION_JSON))

            mockServer.expect(requestTo("http://localhost:8080/book/2"))
                    .andExpect(method(HttpMethod.PUT))
                    .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                    .andExpect(content().string('{"class":"org.grails.datastore.gorm.rest.client.Book","id":2,"pages":null,"title":"The Shining"}'))
                    .andRespond(withSuccess('{"id":2, "title":"The Shining"}', MediaType.APPLICATION_JSON))


            def b = new Book(title: "The Stand")
            def b2 = new Book(title: "The Shining")
            b2.id = 2L
            def ids = Book.withSession { session ->
                def identifiers = Book.saveAll([b, b2])
                session.flush()
                return identifiers
            }


        then:"The number of books that exist is correct"
            b.id == 1L
            ids == [1L, 2L]
            mockServer.verify()

    }

    void "Test the save method issues a POST request with the correct data"() {
        when:"A new entity is saved"
            RestTemplate rt = Book.getRestBuilder().restTemplate
            final mockServer = MockRestServiceServer.createServer(rt)
            mockServer.expect(requestTo("http://localhost:8080/book"))
                    .andExpect(method(HttpMethod.POST))
                    .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                    .andExpect(content().string('{"class":"org.grails.datastore.gorm.rest.client.Book","id":null,"pages":null,"title":"The Stand"}'))
                    .andRespond(withSuccess('{"id":1, "title":"The Stand"}', MediaType.APPLICATION_JSON))

            def b = new Book(title: "The Stand")
            b.save()

        then:"The number of books that exist is correct"
            b.id == 1L
            mockServer.verify()

    }

    void "Test the insert method issues a POST request with the correct data"() {
        when:"A new entity is saved"
            RestTemplate rt = Book.getRestBuilder().restTemplate
            final mockServer = MockRestServiceServer.createServer(rt)
            mockServer.expect(requestTo("http://localhost:8080/book"))
                    .andExpect(method(HttpMethod.POST))
                    .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                    .andExpect(content().string('{"class":"org.grails.datastore.gorm.rest.client.Book","id":1,"pages":null,"title":"The Stand"}'))
                    .andRespond(withSuccess('{"id":1, "title":"The Stand"}', MediaType.APPLICATION_JSON))

            def b = new Book(title: "The Stand")
            b.id = 1L
            b.insert(flush:true)

        then:"The number of books that exist is correct"
            b.id == 1L
            mockServer.verify()

    }

    void "Test the save method issues a PUT request for an update with the correct data"() {
        when:"A new entity is saved"
            RestTemplate rt = Book.getRestBuilder().restTemplate
            final mockServer = MockRestServiceServer.createServer(rt)
            mockServer.expect(requestTo("http://localhost:8080/book/1"))
                    .andExpect(method(HttpMethod.PUT))
                    .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                    .andExpect(content().string('{"class":"org.grails.datastore.gorm.rest.client.Book","id":1,"pages":null,"title":"The Stand"}'))
                    .andRespond(withSuccess('{"id":1, "title":"The Stand"}', MediaType.APPLICATION_JSON))

            def b = new Book(title: "The Stand")
            b.id = 1L
            b.save(flush:true)

        then:"The number of books that exist is correct"
            b.id == 1L
            mockServer.verify()

    }

    void "Test the delete method issues a DELETE request for an update with the correct data"() {
        when:"A new entity is saved"
            RestTemplate rt = Book.getRestBuilder().restTemplate
            final mockServer = MockRestServiceServer.createServer(rt)
            mockServer.expect(requestTo("http://localhost:8080/book/1"))
                    .andExpect(method(HttpMethod.DELETE))
                    .andRespond(withStatus(HttpStatus.NO_CONTENT))

            def b = new Book(title: "The Stand")
            b.id = 1L
            b.delete(flush:true)

        then:"The number of books that exist is correct"
            b.id == 1L
            mockServer.verify()

    }

    void "Test the deleteAll method issues a DELETE request for an update with the correct data"() {
        when:"A new entity is saved"
            RestTemplate rt = Book.getRestBuilder().restTemplate
            final mockServer = MockRestServiceServer.createServer(rt)
            mockServer.expect(requestTo("http://localhost:8080/book/1"))
                    .andExpect(method(HttpMethod.DELETE))
                    .andRespond(withStatus(HttpStatus.NO_CONTENT))

            def b = new Book(title: "The Stand")
            b.id = 1L
            Book.withSession { session ->
                Book.deleteAll([b])
                session.flush()
            }



        then:"The number of books that exist is correct"
            b.id == 1L
            mockServer.verify()

    }
    @Override
    List<Class> getDomainClasses() {
        [Book]
    }
}

@Entity
class Book {
    Long id
    Long version
    String title
    Integer pages

    static mapping = {
//        async false
    }
}
