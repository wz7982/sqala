package sqala.compiletime

import sqala.compiletime.statement.query.Query

import scala.Tuple.*
import scala.compiletime.ops.any.*
import scala.compiletime.ops.boolean.*
import scala.compiletime.ops.int.*
import scala.compiletime.ops.string.*

type NonEmpty[S <: String] = S != ""

type WrapToOption[X] <: Option[?] = X match
    case Option[x] => Option[x]
    case _ => Option[X]

type WrapToTuple[X] <: Tuple = X match
    case h *: t => h *: t
    case _ => Tuple1[X]

type UnwrapFromOption[X] = X match
    case Option[x] => x
    case _ => X

type Distinct[T <: Tuple] <: Tuple = T match
    case h *: t => h *: Distinct[Filter[t, [u] =>> u != h]]
    case EmptyTuple => EmptyTuple

type Combine[X <: Tuple, Y <: Tuple] = Distinct[Concat[X, Y]]

type FlatTupleOption[T <: Tuple] <: Tuple = T match
    case h *: t => h match
        case Option[x] => h *: FlatTupleOption[t]
        case _ => Option[h] *: FlatTupleOption[t]
    case EmptyTuple => EmptyTuple

type InTable[TableNames <: Tuple, ExprTableNames <: Tuple] <: Boolean = ExprTableNames match
    case h *: t => Size[Filter[TableNames, [u] =>> u == h]] > 0 && InTable[TableNames, t]
    case EmptyTuple => true

type ExprsInTable[TableNames <: Tuple, Exprs <: Tuple] <: Boolean = Exprs match
    case h *: t => h match
        case Expr[_, tableNames] => InTable[TableNames, tableNames] && ExprsInTable[TableNames, t]
    case EmptyTuple => true

type FindTypeByName[T <: Tuple, I <: Int, Name <: String] = I >= 0 match
    case true => Tuple.Elem[T, I] match
        case t *: n *: EmptyTuple => n == Name match
            case true => t
            case false => FindTypeByName[T, I - 1, Name]
        case _ => Nothing
    case false => Nothing

type ElementType[T <: Tuple, N <: Tuple, Name <: String] = (T, N) match
    case (t *: tt, n *: nt) => n == Name match
        case true => t
        case false => ElementType[tt, nt, Name]
    case (EmptyTuple, EmptyTuple) => Nothing

type Index[T <: Tuple, Elem, I <: Int] <: Int = T match
    case h *: t => h match
        case Elem => I
        case _ => Index[t, Elem, S[I]]
    case EmptyTuple => 0

type SelectTupleType[Table <: Tuple, TableNames <: Tuple, SelectElems <: Tuple] <: Tuple = SelectElems match
    case h *: t => SelectType[Table, TableNames, h] *: SelectTupleType[Table, TableNames, t]
    case EmptyTuple => EmptyTuple

type SelectOption[Table <: Tuple, TableNames <: Tuple, ExprTableNames <: Tuple] <: Boolean = ExprTableNames match
    case h *: t => Elem[Table, Index[TableNames, h, 0]] match
        case Option[x] => true
        case _ => false || SelectOption[Table, TableNames, t]
    case EmptyTuple => false

type SelectType[Table <: Tuple, TableNames <: Tuple, Item] = Item match
    case Expr[a, tableNames] => tableNames match
        case EmptyTuple => a
        case _ => SelectOption[Table, TableNames, tableNames] match
            case false => a
            case true => a match
                case Option[aa] => Option[aa]
                case _ => Option[a]
    case SelectItem[a, tableNames, _] => SelectType[Table, TableNames, Expr[a, tableNames]]

type GroupTupleType[Table <: Tuple, TableNames <: Tuple, GroupElems <: Tuple] <: Tuple = GroupElems match
    case h *: t => GroupType[Table, TableNames, h] *: GroupTupleType[Table, TableNames, t]
    case EmptyTuple => EmptyTuple

type GroupType[Table <: Tuple, TableNames <: Tuple, Item] = Item match
    case Expr[a, tableNames] => tableNames match
        case EmptyTuple => a
        case _ => SelectOption[Table, TableNames, tableNames] match
            case false => a
            case true => a match
                case Option[aa] => Option[aa]
                case _ => Option[a]

type HasAliasName[T <: Tuple] <: Boolean = T match
    case SelectItem[_, _, n] *: t => true && HasAliasName[t]
    case Column[_, _, n] *: t => true && HasAliasName[t]
    case PrimaryKey[_, _, n] *: t => true && HasAliasName[t]
    case EmptyTuple => true
    case _ => false

type ExtractAliasNames[T <: Tuple] <: Tuple = T match
    case SelectItem[_, _, n] *: t => n *: ExtractAliasNames[t]
    case Column[_, _, n] *: t => n *: ExtractAliasNames[t]
    case PrimaryKey[_, _, n] *: t => n *: ExtractAliasNames[t]
    case _ => EmptyTuple

type TupleAliasNames[T <: Tuple] <: Tuple = HasAliasName[T] match
    case true => ExtractAliasNames[T]
    case false => EmptyTuple

type JoinTableType[X <: AnyTable[?, ?], Y <: AnyTable[?, ?]] <: Tuple = (X, Y) match
    case (JoinTable[_, _, x], JoinTable[_, _, y]) => Concat[x, y]
    case (JoinTable[_, _, x], _) => Concat[x, Tuple1[Y]]
    case (_, JoinTable[_, _, y]) => Concat[Tuple1[X], y]
    case _ => (X, Y)

type FetchTableNames[E <: Expr[?, ?]] <: Tuple = E match
    case Expr[_, n] => n

type Repeat[T <: Tuple] <: Boolean = T match
    case h *: t => 
        Size[Filter[T, [u] =>> u == h]] > 1 match
            case true => true
            case false => Repeat[t]
    case EmptyTuple => false

type ConcatTables[X <: Tuple, Y <: Tuple, F[_ <: Tuple]] <: Tuple = Y match
    case h *: t => h match
        case F[tableNames] => ConcatTables[Combine[X, tableNames], t, F]
    case EmptyTuple => X

type CanUnion[X <: Tuple, Y <: Tuple] <: Boolean = (X, Y) match
    case (a *: at, b *: bt) => CanUnionTo[a, b] && CanUnion[at, bt]
    case (EmptyTuple, EmptyTuple) => true

type CanUnionTo[A, B] <: Boolean = A match
    case B => true
    case Option[B] => true
    case UnwrapFromOption[B] => true
    case _ => false

type UnionTuple[X <: Tuple, Y <: Tuple] <: Tuple = (X, Y) match
    case (a *: at, b *: bt) => UnionTo[a, b] *: UnionTuple[at, bt]
    case (EmptyTuple, EmptyTuple) => EmptyTuple

type UnionTo[A, B] = A match
    case B => B
    case Option[B] => A
    case UnwrapFromOption[B] => B

type QueryType[Q <: Query[?, ?]] = Q match
    case Query[t, _] => t

type StringToTuple[S <: String, I <: Int] <: Tuple = Length[S] > I match
    case true => compiletime.ops.any.ToString[CharAt[S, I]] *: StringToTuple[S, compiletime.ops.int.+[I, 1]]
    case false => EmptyTuple

type LowerCase[S <: String] = S match
    case "A" => "a"
    case "B" => "b"
    case "C" => "c"
    case "D" => "d"
    case "E" => "e"
    case "F" => "f"
    case "G" => "g"
    case "H" => "h"
    case "I" => "i"
    case "J" => "j"
    case "K" => "k"
    case "L" => "l"
    case "M" => "m"
    case "N" => "n"
    case "O" => "o"
    case "P" => "p"
    case "Q" => "q"
    case "R" => "r"
    case "S" => "s"
    case "T" => "t"
    case "U" => "u"
    case "V" => "v"
    case "W" => "w"
    case "X" => "x"
    case "Y" => "y"
    case "Z" => "z"
    case _ => S

type UpperCase[S <: String] = S match
    case "a" => "A"
    case "b" => "B"
    case "c" => "C"
    case "d" => "D"
    case "e" => "E"
    case "f" => "F"
    case "g" => "G"
    case "h" => "H"
    case "i" => "I"
    case "j" => "J"
    case "k" => "K"
    case "l" => "L"
    case "m" => "M"
    case "n" => "N"
    case "o" => "O"
    case "p" => "P"
    case "q" => "Q"
    case "r" => "R"
    case "s" => "S"
    case "t" => "T"
    case "u" => "U"
    case "v" => "V"
    case "w" => "W"
    case "x" => "X"
    case "y" => "Y"
    case "z" => "Z"
    case _ => S

type UpperCaseInitial[S <: String] = S match
    case "" => ""
    case _ => compiletime.ops.string.+[UpperCase[compiletime.ops.any.ToString[CharAt[S, 0]]], Substring[S, 1, Length[S]]]

type TupleToTokens[T <: Tuple, S <: String] <: Tuple = T match
    case h *: t => h match
        case "A" | "B" | "C" | "D" | "E" | "F" | "G" | "H" | "I" | "J" | "K" | "L" | "M" | "N" | "O" | "P" | "Q" | "R" | "S" | "T" | "U" | "V" | "W" | "X" | "Y" | "Z" =>
            S *: TupleToTokens[LowerCase[compiletime.ops.any.ToString[h]] *: t, ""]
        case _ => TupleToTokens[t, compiletime.ops.string.+[S, compiletime.ops.any.ToString[h]]]
    case EmptyTuple => S *: EmptyTuple

type NamedQueryTokens[T <: Tuple, S <: String] <: Tuple = T match
    case "find" *: "by" *: t => "by" *: NamedQueryTokens[t, ""]
    case "not" *: "in" *: t => S *: "notIn" *: NamedQueryTokens[t, ""]
    case "not" *: "like" *: t => S *: "notLike" *: NamedQueryTokens[t, ""]
    case "not" *: "between" *: t => S *: "notBetween" *: NamedQueryTokens[t, ""]
    case "starting" *: "with" *: t => S *: "startingWith" *: NamedQueryTokens[t, ""]
    case "ending" *: "with" *: t => S *: "endingWith" *: NamedQueryTokens[t, ""]
    case "is" *: "null" *: t => S *: "isNull" *: NamedQueryTokens[t, ""]
    case "is" *: "not" *: "null" *: t => S *: "isNotNull" *: NamedQueryTokens[t, ""]
    case "not" *: "null" *: t => S *: "isNotNull" *: NamedQueryTokens[t, ""]
    case "greater" *: "than" *: "equal" *: t => S *: "greaterThanEqual" *: NamedQueryTokens[t, ""]
    case "less" *: "than" *: "equal" *: t => S *: "lessThanEqual" *: NamedQueryTokens[t, ""]
    case "greater" *: "than" *: t => S *: "greaterThan" *: NamedQueryTokens[t, ""]
    case "less" *: "than" *: t => S *: "lessThan" *: NamedQueryTokens[t, ""]
    case "order" *: "by" *: t => S *: EmptyTuple
    case h *: t => h match
        case "and" | "or" | "not" | "in" | "between" => S *: h *: NamedQueryTokens[t, ""]
        case "is" | "equals" => S *: NamedQueryTokens[t, ""]
        case "like" | "containing" => S *: "like" *: NamedQueryTokens[t, ""]
        case _ => S match
            case "" => NamedQueryTokens[t, h]
            case _ => NamedQueryTokens[t, compiletime.ops.string.+[S, UpperCaseInitial[h]]]
    case EmptyTuple => S *: EmptyTuple

type StringToNamedQueryTokens[S <: String] = NamedQueryTokens[TupleToTokens[StringToTuple[S, 0], ""], ""]

type NamedQueryArgsType[ElementTypes <: Tuple, ElementLabels <: Tuple, Tokens <: Tuple] <: Tuple = Tokens match
    case h *: t => h match
        case "by" | "and" | "or" => t match
            case name *: tt => tt match
                case ("in" | "notIn") *: ttt => 
                    List[ElementType[ElementTypes, ElementLabels, name]] *: NamedQueryArgsType[ElementTypes, ElementLabels, ttt]
                case ("like" | "notLike" | "startingWith" | "endingWith") *: ttt =>
                    String *: NamedQueryArgsType[ElementTypes, ElementLabels, ttt]
                case ("greaterThan" | "lessThan" | "greaterThanEqual" | "lessThanEqual") *: ttt =>
                    ElementType[ElementTypes, ElementLabels, name] *: NamedQueryArgsType[ElementTypes, ElementLabels, ttt]
                case ("between" | "notBetween") *: ttt =>
                    ElementType[ElementTypes, ElementLabels, name] *: ElementType[ElementTypes, ElementLabels, name] *: NamedQueryArgsType[ElementTypes, ElementLabels, ttt]
                case ("isNull" | "isNotNull") *: ttt =>
                    NamedQueryArgsType[ElementTypes, ElementLabels, ttt]
                case _ => ElementType[ElementTypes, ElementLabels, name] *: NamedQueryArgsType[ElementTypes, ElementLabels, tt]
        case _ => NamedQueryArgsType[ElementTypes, ElementLabels, t]
    case EmptyTuple => EmptyTuple