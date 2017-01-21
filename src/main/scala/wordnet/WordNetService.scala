package wordnet

import java.io.FileInputStream

import net.didion.jwnl.JWNL
import net.didion.jwnl.data.POS
import net.didion.jwnl.dictionary.Dictionary

import scala.collection.immutable.Seq

/**
  * Created by faiaz on 21.01.17.
  */
class WordNetService {

  JWNL.initialize(new FileInputStream("src/main/resources/wordnet.xml"))
  private val dict = Dictionary.getInstance()

  def synonyms(lemma: String, pos: POS): Seq[String] = {
    val word = dict.getIndexWord(pos, lemma)
    if (word == null) Nil
    else word.getSenses.toList.map(_.getWords).flatMap(_.map(_.getLemma)).distinct
  }
}
