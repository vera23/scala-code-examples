package orb.data.writer

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import orb.core.RequestPathSegment
import io.circe.Json.{JArray, JBoolean, JNull, JNumber, JObject, JString}
import io.circe.JsonNumber._
import io.circe.Json.Folder
import orb.api.http.data._
import orb.tree._
import zio.logging.Logging
import java.util.Date
import com.datastax.driver.core.LocalDate
import io.circe.Encoder.encodeJsonNumber
import orb.core._

case class FoldResult(path:Path = Nil, result:WriteRequestParsingResult = Nil, parentIdsAndKeyValues: Map[Short, String] = Map.empty) {
  def +(other:FoldResult):FoldResult = copy(result = result ++ other.result)
}

given WriteRequestAttributesDecoder(using rootNode: TreeNode, enums:Seq[EnumRef]): Decoder[WriteRequestAttributes] with

  def apply(c: HCursor): Decoder.Result[WriteRequestAttributes] = {

    case class WriteRequestFolder(treeNode: TreeNode, fold:FoldResult = FoldResult(), isCollectionMode: Boolean = false, collectionNodeIdAndKey: Map[Short, String] = Map.empty) extends Folder[FoldResult] {
      def onNull: FoldResult = result(Right(FieldData(fold.path, treeNode.storageLocation, null, fold.parentIdsAndKeyValues, collectionKeyAndValue)))

      def onBoolean(value: Boolean): FoldResult = resultOrError(treeNode, value)

      def onNumber(value: JsonNumber): FoldResult = resultOrError(treeNode, value)

      def onString(value: String): FoldResult = resultOrError(treeNode, value)

      def onArray(value: Vector[Json]): FoldResult = resultOrError(treeNode, value.map(v => {
            v.foldWith(WriteRequestFolder(treeNode, fold, false, Map.empty)).result
          }).flatten.toList
      )

      def onObject(value: JsonObject): FoldResult = value.toMap.map {
          case (k, v) => {
            if (isCollectionMode)
              Validate.validateAndTransform(k, treeNode, enums).fold(e => result(Left(e)), key =>
                  v.foldWith(WriteRequestFolder(treeNode, fold.copy(path = fold.path :+ RequestPathSegment.Collection(treeNode.name, Option(key.asInstanceOf[String])), parentIdsAndKeyValues = parentIdsAndKeyValues(treeNode)), collectionNodeIdAndKey = Map(treeNode.id -> key.asInstanceOf[String]))))
            else
              {
                if (treeNode.hasChild(k)) {
                  val child = treeNode.getChildByName(k).get

                  if (child.isCollection)
                    v.foldWith(WriteRequestFolder(child, fold.copy(path = fold.path), isCollectionMode = true))
                  else
                    v.foldWith(WriteRequestFolder(child, fold.copy(path = fold.path :+ RequestPathSegment.Node(k)), isCollectionMode = false))
                }
                else {
                    result(Left(s"wrong path ${path(treeNode)}.$k"))
                }
              }
          }
        }.toList.reduceOption(_ + _).getOrElse(fold)

      protected def pathForErrorMessage(path: List[RequestPathSegment]): String = path.map {
        case RequestPathSegment.Node(v) => v
        case RequestPathSegment.Collection(v, _) => v
      }.mkString(".")

      protected def resultOrError[A](treeNode: TreeNode, value: A): FoldResult = result(Validate.validateAndTransform(value, treeNode, enums).fold(e => Left(e), value => Right(FieldData(fold.path, treeNode.storageLocation, value, fold.parentIdsAndKeyValues, collectionKeyAndValue))))

      protected def result(res: Either[String, FieldData]) = fold.copy(result = fold.result :+ res)

      def treeNodeHasCollectionParents(treeNode: TreeNode) : Boolean = treeNode.findFirstParent(_.isCollection).isDefined

      def parentIdsAndKeyValues(treeNode: TreeNode): Map[Short, String] =
        if(treeNodeHasCollectionParents(treeNode)) fold.parentIdsAndKeyValues ++ collectionNodeIdAndKey else fold.parentIdsAndKeyValues

      def collectionKey:Option[RequestPathSegment] = fold.path.findLast {
        case _: RequestPathSegment.Collection => true
        case _ => false
      }

      def collectionKeyAndValue: Map[String, String] = collectionKey match {
          case Some(RequestPathSegment.Collection(name, Some(key))) => Map("key" -> key)
          case _ => Map.empty
      }
    }

    Right(WriteRequestAttributes(c.value.foldWith(WriteRequestFolder(rootNode)).result))
  }
