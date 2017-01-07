package linguistics

/**
  * Created by faiaz on 08.01.17.
  */
sealed trait SentencesParts
case class Noun(noun: String) extends SentencesParts