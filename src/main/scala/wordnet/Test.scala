package wordnet

import java.io.FileInputStream

import net.didion.jwnl.JWNL
import net.didion.jwnl.data.POS
import net.didion.jwnl.dictionary.Dictionary

/**
  * Created by faiaz on 20.01.17.
  */
object Test extends App {

  JWNL.initialize(new FileInputStream("src/main/resources/properties.xml"))
  val dict = Dictionary.getInstance()
  dict.getSynsetAt(POS.NOUN, 4)
}
