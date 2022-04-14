package orb.data.writer

import cats.Traverse.nonInheritedOps.toTraverseOps
import com.datastax.driver.core.LocalDate
import io.circe.JsonNumber
import orb.api.http.data.FieldData
import orb.core.*
import orb.tree.{NodeType, TreeNode}

import java.math.BigDecimal as JavaBigDecimal
import java.text.{DateFormat, SimpleDateFormat}
import java.time.LocalDate as JavaLocalDate
import java.time.format.DateTimeFormatter
import java.util.Date
import scala.collection.JavaConverters.*

type Error = String

trait Validator {
  type Out
  def validateAndTransform[A](v:A,treeNode:TreeNode):Either[Error, Out]
}

object Validate {
  def createValidator(nodeType: NodeType, enums: Seq[EnumRef]): Validator = nodeType match {
    case NodeType.Boolean => BooleanValidator
    case NodeType.String => StringValidator
    case NodeType.Int => IntValidator
    case NodeType.Date => DateValidator
    case NodeType.Numeric => NumericValidator
    case NodeType.Enum(name) => EnumValidator(name, enums)
    case NodeType.List(listNodeType) => ListValidator(listNodeType, enums)
    case NodeType.Collection => CollectionKeyValidator
    case NodeType.Branch => BranchValidator
  }
  
  def validateAndTransform[A](a: A, tree: TreeNode, enums: Seq[EnumRef]): Either[Error,  Validator#Out]= createValidator(tree.nodeType, enums).validateAndTransform(a, tree)
}

object BooleanValidator extends Validator {
  override type Out = Boolean
  def validateAndTransform[A](v:A,treeNode:TreeNode): Either[Error, Out] = {
    v match {
      case v: Boolean => Right(v)
      case v => Left(s"value $v on path ${path(treeNode)} should be boolean")
    }
  }
}

object StringValidator extends Validator{
  override type Out = String
  def validateAndTransform[A](v: A, treeNode: TreeNode):Either[Error, Out] = v match {
    case v: String => Right(v)
    case v => Left(s"value $v on path ${path(treeNode)} should be string")
  }
}

object IntValidator extends Validator {
  override type Out = Int

  def validateAndTransform[A](v: A, treeNode: TreeNode):Either[Error, Out] = v match {
    case v: JsonNumber => if (v.toInt.isDefined) Right(v.toInt.get) else Left("Coundn't convert to Int")
    case v => Left(s"value $v on path ${path(treeNode)} should be string")
  }
}

object DateValidator extends Validator {
  override type Out = LocalDate

  def validateAndTransform[A](v: A, treeNode: TreeNode) = v match {
    case v: String => try {
      Right(LocalDate.fromDaysSinceEpoch(JavaLocalDate.parse(v, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay.asInstanceOf[Int]))
    } catch {
      case e => Left(s"value $v on path ${path(treeNode)} should be date")
    }
    case v => Left(s"value $v on path ${path(treeNode)} should be string")
  }
}


object BranchValidator extends Validator {
  def validateAndTransform[A](v: A, treeNode: TreeNode) = Left(s"Branch node ${treeNode.name} can not have any value, value = $v), path ${path(treeNode)}")
}

object CollectionKeyValidator extends Validator {
  override type Out = String

  def validateAndTransform[A](v: A, treeNode: TreeNode): Either[Error, String] = if (isValidCollectionKey(v.toString)) Right(v.toString) else Left(s"not valid collection $v on path ${path(treeNode)}")
}


object NumericValidator extends Validator {

  override type Out = JavaBigDecimal

  def validateAndTransform[A](v: A, treeNode: TreeNode) = v match {
    case v: JsonNumber=> v.toBigDecimal match {
      case Some(v) => Right(v.bigDecimal)
      case _ => Left(s"Wrong number format on path ${path(treeNode)}")
    }
    case v => Left(s"value $v on path ${path(treeNode)} should be numeric")
  }
}

case class EnumValidator(name: String, enums: Seq[EnumRef]) extends Validator {

  override type Out = CID

  def validateAndTransform[A](v: A, treeNode: TreeNode) = enums.find(_.name == name) match {
    case Some(entityRef) if v.isInstanceOf[String] => entityRef.values.get(v.asInstanceOf[String]) match {
      case Some(enumValue) => Right(enumValue.cid)
      case _ => Left(s"enum $name has no value $v on path ${path(treeNode)}")
    }
    case _ => Left(s"there is no enum $name for value $v on path ${path(treeNode)} ")
  }
}

case class ListValidator(nodeType: NodeType, enums: Seq[EnumRef]) extends Validator {

  override type Out = Any

  def validateAndTransformList[A](v: List[Either[Error, FieldData]], treeNode: TreeNode) = v.sequence match {
      case Left(_) => Left(s"list has values with wrong types on path ${path(treeNode)}")
      case Right(fieldDataList) => Right((fieldDataList map (_.value)).asJava)
  }

  def validateAndTransform[A](v: A, treeNode: TreeNode) = v match {
    case list: List[_] => validateAndTransformList(list.asInstanceOf[List[Either[Error, FieldData]]], treeNode)
    case _ =>  Validate.createValidator(nodeType, enums).validateAndTransform(v, treeNode)
  }
}

def path(treeNode: TreeNode): String = treeNode.path.map(_.seg).mkString(".")

