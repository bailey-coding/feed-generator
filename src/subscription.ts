import * as natural from 'natural'

import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

const Analyzer = natural.SentimentAnalyzer
const stemmer = natural.PorterStemmer
const analyzer = new Analyzer('English', stemmer, 'afinn')

function interpretSentiment(score: number) {
  if (score > 0.5) return 'Strongly Positive'
  if (score > 0) return 'Positive'
  if (score === 0) return 'Neutral'
  if (score > -0.5) return 'Negative'
  return 'Strongly Negative'
}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    // for (const post of ops.posts.creates) {
    //   console.log(post.record.text)
    // }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only queer-related posts
        return (
          create.record.text.toLowerCase().includes('queer') ||
          create.record.text.toLowerCase().includes('trans')
        )
      })
      .filter((create) => {
        return create.record.langs?.indexOf('en') !== -1
      })
      .filter((create) => {
        const result = analyzer.getSentiment(create.record.text.split(' '))
        const humanReadable = interpretSentiment(result)
        if (result > 0.5) {
          console.log(`message: Score is ${result} - ${humanReadable}
            post: ${create.record.text}\n`)
        } else {
          console.log(`Ignored message, result=${humanReadable}`)
        }
        return result > 0.5
      })
      .map((create) => {
        // console.table(create)
        // console.log(create.record.text)
        // map alf-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
