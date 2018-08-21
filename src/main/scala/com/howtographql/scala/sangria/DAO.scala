package com.howtographql.scala.sangria

import com.howtographql.scala.sangria.DBSchema.{Links, Users, Votes}
import com.howtographql.scala.sangria.models.{AuthProviderSignupData, Link, User, Vote}
import sangria.execution.deferred.{RelationIds, SimpleRelation}
import slick.jdbc.H2Profile.api._

class DAO(db: Database) {

  def createUser(name: String, authProvider: AuthProviderSignupData) = {
    val newUser = User(0, name, authProvider.email.email, authProvider.email.password)

    val insertAndReturnUserQuery = (Users returning Users.map(_.id)) into {
      (user, id) => user.copy(id = id)
    }

    db.run{
      insertAndReturnUserQuery += newUser
    }
  }

  def authenticate(email: String, password: String) = db.run {
    Users.filter(u => u.email === email && u.password === password).result.headOption
  }

  def getUsers(ids: Seq[Int]) = {
    db.run(
      Users.filter(_.id inSet ids).result
    )
  }

  def createVote(linkId: Int, userId: Int) = {
    val insertAndReturnVoteQuery = (Votes returning Votes.map(_.id)) into {
      (vote, id) => vote.copy(id = id)
    }
    db.run {
      insertAndReturnVoteQuery += Vote(0, userId, linkId)
    }
  }

  def getVotes(ids: Seq[Int]) = {
    db.run(
      Votes.filter(_.id inSet ids).result
    )
  }

  def getVotesByRelationIds(rel: RelationIds[Vote]) =
    db.run(
      Votes.filter { vote =>
        rel.rawIds.collect({
          case (SimpleRelation("byUser"), ids: Seq[Int]) => vote.userId inSet ids
          case (SimpleRelation("byLink"), ids: Seq[Int]) => vote.linkId inSet ids
        }).foldLeft(true: Rep[Boolean])(_ || _)

      } result
    )

  def createLink(url: String, description: String, postedBy: Int) = {

    val insertAndReturnLinkQuery = (Links returning Links.map(_.id)) into {
      (link, id) => link.copy(id = id)
    }
    db.run {
      insertAndReturnLinkQuery += Link(0, url, description, postedBy)
    }
  }

  def allLinks = db.run(Links.result)

  def getLinks(ids: Seq[Int]) = db.run(
    Links.filter(_.id inSet ids).result
  )

  def getLinksByUserIds(ids: Seq[Int]) = {
    db.run (
      Links.filter(_.postedBy inSet ids).result
    )
  }
}
