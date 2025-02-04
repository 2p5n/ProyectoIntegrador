package utilidades

object EscapeCaracteresEspeciales {

  // Función para limpiar JSON mal formados, ajustando comillas, comas y otros errores de formato.
  def limpiarJson(jsonStr: String): String = {
    val trimmed = jsonStr.trim  // Elimina los espacios al principio y final de la cadena JSON

    // Si la cadena está vacía o es "null", devolvemos "null"
    if (trimmed.isEmpty || trimmed.equalsIgnoreCase("null")) "null"
    else {
      trimmed
        // Eliminar comillas dobles mal ubicadas antes de corchetes/llaves
        .replaceAll("\"\\s*\\}", "}")  // Elimina comillas dobles antes de una llave de cierre
        .replaceAll("\"\\s*\\]", "]")  // Elimina comillas dobles antes de un corchete de cierre
        // Corregir casos específicos de errores de formato
        .replaceAll("\"}]", "}]")  // Elimina comillas dobles innecesarias antes de corchetes y llaves
        .replaceAll("\"\\)", ")")  // Elimina comillas dobles antes de un paréntesis
        // Limpieza general de caracteres especiales y formateo
        .replace("'", "\"")  // Cambia comillas simples por dobles
        .replace(";;", ";")  // Elimina puntos y coma dobles
        .replace(",}", "}")  // Elimina comas antes de una llave de cierre
        .replace(",]", "]")  // Elimina comas antes de un corchete de cierre
        .replace(";", "")  // Elimina puntos y coma innecesarios
        .replace("\"s", "'s")  // Cambia comillas dobles seguidas de "s" por comillas simples
        .replace("\\\"", "\"")  // Elimina barras invertidas antes de comillas dobles
        .replace("\\n", "\n")  // Reemplaza \n con saltos de línea
        .replace("\\t", "\t")  // Reemplaza \t con tabulaciones
        .replace("None", "null")  // Reemplaza "None" con "null"
        // Asegurar formato de array/objeto válido
        .replaceAll("^\\[?\\s*\\{", "[{")  // Elimina espacios al principio antes de una llave en un array
        .replaceAll("\\}\\s*\\]?$", "}]")  // Asegura que un objeto termine correctamente en un array
    }
  }

  // Función para limpiar JSON con datos de "crew" (equipo de producción) de manera específica
  def limpiarJsonCrew(jsonStr: String): String = {
    val trimmed = jsonStr.trim  // Elimina los espacios al principio y final de la cadena JSON

    // Si la cadena está vacía o es "null", devolvemos "null"
    if (trimmed.isEmpty || trimmed.toLowerCase == "null") return "null"

    // Eliminar caracteres no válidos como ; al final de las cadenas
    val sinEscape = trimmed
      .replace("'", "\"")  // Cambia comillas simples por dobles
      .replace(";;", ";")  // Elimina dobles puntos y coma
      .replace(",}", "}")  // Elimina comas antes de una llave de cierre
      .replace(",]", "]")  // Elimina comas antes de un corchete de cierre
      .replace("},]", "}]")  // Elimina comas innecesarias entre objetos en una lista
      .replace("\"\"}", "\"}")  // Corrige comillas dobles seguidas de una llave de cierre
      .replace(";", "")  // Elimina puntos y coma innecesarios
      .replace("\\\"", "\"")  // Elimina barras invertidas antes de comillas dobles
      .replace("\\n", "\n")  // Reemplaza \n por saltos de línea
      .replace("\\t", "\t")  // Reemplaza \t por tabulaciones
      .replace("\\'", "\"")  // Cambia \' por comillas dobles
      .replaceAll("(?<!\\w)'(?!\\w)", "\"")  // Reemplaza comillas simples por dobles si no están dentro de una palabra
      .replaceAll("\\\\", "")  // Elimina barras invertidas dobles
      .replaceAll("\\s*:\\s*", ":")  // Elimina espacios alrededor de los dos puntos
      .replaceAll("\\s*,\\s*", ",")  // Elimina espacios alrededor de las comas
      .replaceAll("\\s*\\{\\s*", "{")  // Elimina espacios después de llaves de apertura
      .replaceAll("\\s*\\}\\s*", "}")  // Elimina espacios antes de llaves de cierre
      .replaceAll("\\s*\\[\\s*", "[")  // Elimina espacios después de corchetes de apertura
      .replaceAll("\\s*\\]\\s*", "]")  // Elimina espacios antes de corchetes de cierre
      .replace("None", "null")  // Reemplaza "None" con "null"

    // Validación y corrección de las llaves de apertura y cierre de objetos o arrays
    if (sinEscape.startsWith("{") && !sinEscape.endsWith("\"}")) {
      sinEscape + "\"}"  // Si empieza con un objeto pero no termina correctamente, añadimos el cierre
    } else if (sinEscape.startsWith("{") && !sinEscape.endsWith("}")) {
      sinEscape + "}"  // Si empieza con un objeto y falta el cierre, lo añadimos
    } else if (sinEscape.startsWith("[") && sinEscape.endsWith("]")) {
      sinEscape  // Si empieza y termina con corchetes, ya está bien formateado
    } else if (sinEscape.startsWith("[") && sinEscape.endsWith(",{\"iso_31]")) {
      sinEscape.replaceAll(",\\{\"iso_31.*?]$", "]")  // Corregir ciertos casos de objetos mal formateados
    } else if (sinEscape.startsWith("[") && sinEscape.endsWith(",{\"id\":")) {
      sinEscape.replaceAll(",\\{\"id\".*?$", "]")  // Corregir objetos mal formateados con ID
    } else if (sinEscape.startsWith("[") && !sinEscape.endsWith("]")) {
      sinEscape + "]"  // Si empieza con un array pero no termina correctamente, añadimos el cierre
    } else {
      "null"  // Si no corresponde a ningún formato válido, devolvemos "null"
    }
  }

  // Función para limpiar JSON con datos de "ratings" (calificaciones)
  def limpiarJsonRatings(jsonStr: String): String = {
    val trimmed = jsonStr.trim  // Elimina los espacios al principio y final de la cadena JSON

    // Si la cadena está vacía o es "null", devolvemos "null"
    if (trimmed.isEmpty || trimmed.toLowerCase == "null") return "null"

    // Eliminar caracteres no válidos como ; al final de las cadenas
    val sinEscape = trimmed
      .replace("'", "\"")  // Cambia comillas simples por dobles
      .replace(";;", ";")  // Elimina dobles puntos y coma
      .replace(",}", "}")  // Elimina comas antes de una llave de cierre
      .replace(",]", "]")  // Elimina comas antes de un corchete de cierre
      .replace("},]", "}]")  // Elimina comas innecesarias entre objetos en una lista
      .replace("\"\"}", "\"}")  // Corrige comillas dobles seguidas de una llave de cierre
      .replace(";", "")  // Elimina puntos y coma innecesarios
      .replace("\\\"", "\"")  // Elimina barras invertidas antes de comillas dobles
      .replace("\\n", "\n")  // Reemplaza \n por saltos de línea
      .replace("\\t", "\t")  // Reemplaza \t por tabulaciones
      .replace("\\'", "\"")  // Cambia \' por comillas dobles
      .replace("None", "null")  // Reemplaza "None" con "null"

    // Validación y corrección de las llaves de apertura y cierre de objetos o arrays
    if (sinEscape.startsWith("{") && !sinEscape.endsWith("}")) {
      sinEscape + "}"  // Si empieza con un objeto pero no termina correctamente, añadimos el cierre
    } else if (sinEscape.startsWith("[") && sinEscape.endsWith("]")) {
      sinEscape  // Si empieza y termina con corchetes, ya está bien formateado
    } else if (sinEscape.startsWith("[") && !sinEscape.endsWith("]")) {
      sinEscape + "]"  // Si empieza con un array pero no termina correctamente, añadimos el cierre
    } else {
      "null"  // Si no corresponde a ningún formato válido, devolvemos "null"
    }
  }
}
