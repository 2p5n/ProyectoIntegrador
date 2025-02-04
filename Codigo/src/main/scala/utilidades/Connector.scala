package utilidades

import doobie.*  // Importa la librería de Doobie, utilizada para trabajar con bases de datos en Scala
import doobie.implicits.*  // Importa implicaciones para trabajar con SQL de forma más sencilla

import cats.*  // Importa las librerías de Cats para trabajar con tipos funcionales y efectos
import cats.effect.*  // Importa Cats Effect para efectos asincrónicos y manejo de recursos
import cats.implicits.*  // Importa las implicaciones de Cats para trabajar con estructuras funcionales
import cats.effect.unsafe.implicits.global  // Importa las implicaciones necesarias para ejecutar IO de forma segura

object Connector {

  // Define un transactor (un objeto de Doobie que maneja la conexión a la base de datos)
  val xa = Transactor.fromDriverManager[IO](
    driver = "com.mysql.cj.jdbc.Driver", // El driver JDBC que se usará para la conexión con MySQL
    url = "jdbc:mysql://localhost:3306/prueba", // La URL de la base de datos (en este caso, se conecta a MySQL en localhost, base de datos "prueba")
    user = "user", // El nombre de usuario de la base de datos
    password = "", // La contraseña de la base de datos (en este caso, se deja vacía)
    logHandler = None // No se proporciona un manejador de logs (en este caso no se registra la información de logs)
  )
}
