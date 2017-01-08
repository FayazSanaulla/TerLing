package linguistics

/**
  * Created by faiaz on 08.01.17.
  */
sealed abstract class SentencesParts { val symbol: String }

//Іменник
case class Noun(noun: String) extends SentencesParts {
  override val symbol: String = "NN"
}

//Дієслово
case class Verb(verb: String) extends SentencesParts {
  override val symbol: String = "VB"
}

//Дієслово 3 особи однини теперішнього часу
case class Verb3rdSinglePresent(verb3rdSP: String) extends SentencesParts {
  override val symbol: String = "VBZ"
}

//Дієслово не 3 особи однини теперішнього часу
case class VerbNon3rdSinglePresent(verbN3rd: String) extends SentencesParts {
  override val symbol: String = "VBP"
}

//Дієприкметник минулого часу
case class PastParticiple(pPart: String) extends SentencesParts {
  override val symbol: String = "VBN"
}

//Герундій
case class Gerund(gerund: String) extends SentencesParts {
  override val symbol: String = "VBG"
}

//Прикметник
case class Adjective(adjective: String) extends SentencesParts {
  override val symbol: String = "JJ"
}

//Наріччя
case class Adverb(adverb: String) extends SentencesParts {
  override val symbol: String = "RB"
}

//Іменник множина
case class NounPlural(nounPlural: String) extends SentencesParts {
  override val symbol: String = "NNS"
}

//З'єднання
case class Conjunction(conjunction: String) extends SentencesParts {
  override val symbol: String = "IN"
}

//Дата
case class Date(date: String) extends SentencesParts {
  override val symbol: String = "CD"
}

//Іменник, власна назва, однина
case class ProperNounSingle(pNounSingle: String) extends SentencesParts {
  override val symbol: String = "NNP"
}

//Іменник, власна назва, множина
case class ProperNounPlural(pNounPlural: String) extends SentencesParts {
  override val symbol: String = "NNPS"
}

case class Unknown(unknown: String, override val symbol: String) extends SentencesParts