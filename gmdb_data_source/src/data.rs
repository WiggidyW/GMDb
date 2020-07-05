use bytes::{Bytes, BytesMut};
use futures::stream::Stream;
use std::task::{Context, Poll};
use std::pin::Pin;
use std::iter::{Peekable, FromIterator};

#[derive(Debug, PartialEq, Eq)]
pub enum Kind {
    TitlePrincipals,
    NameBasics,
    TitleAkas,
    TitleBasics,
    TitleCrew,
    TitleEpisode,
    TitleRatings,
}

pub struct Chunk(Bytes);

pub enum Value {
    Partial(Bytes),
    Full(Bytes),
}

pub struct FullValue(Bytes);

pub struct BufferedValueStream<S> {
    stream: S,
    chunk: Peekable<Chunk>,
    partial_buf: Vec<Bytes>,
}

impl Iterator for Chunk {
    type Item = Value;
    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        match (
            self.0
                .iter()
                .enumerate()
                .skip(1)
                .filter_map(|(i, &b)| match b == b'\t' || b == b'\n' {
                    true => Some(i),
                    false => None,
                })
                .next(),
            (&self.0)[0] == b'\t' || (&self.0)[0] == b'\n',
        ) {
            (Some(i), true) => Some(Value::Full(self.0.split_to(i).slice(1..))),
            (Some(i), false) => Some(Value::Partial(self.0.split_to(i))),
            (None, true) => Some(Value::Partial(self.0.split_to(self.0.len()).slice(1..))),
            (None, false) => Some(Value::Partial(self.0.split_to(self.0.len()))),
        }
    }
}

impl Value {
    fn unwrap(self) -> Bytes {
        match self {
            Self::Partial(b) => b,
            Self::Full(b) => b,
        }
    }
}

impl<S> BufferedValueStream<S> {
    fn take_buf(&mut self) -> FullValue {
        match self.partial_buf.len() {
            0 => return FullValue(Bytes::new()),
            1 => return FullValue(self.partial_buf.pop().unwrap()),
            _ => FullValue(BytesMut::from_iter(self.partial_buf
                        .drain(..)
                        .map(|b| b.into_iter())
                        .flatten()
                    )
                    .freeze()
                ),
        }
    }
}

impl<S> Iterator for BufferedValueStream<S> {
    type Item = FullValue;
    fn next(&mut self) -> Option<Self::Item> {
        match self.chunk.peek() {
            Some(Value::Full(_)) if self.partial_buf.len() > 0 => return Some(self.take_buf()),
            Some(Value::Full(_)) => return Some(FullValue(self.chunk.next().unwrap().unwrap())),
            Some(Value::Partial(b)) if b.is_empty() => { self.chunk.next(); },
            Some(Value::Partial(_)) => self.partial_buf.push(self.chunk.next().unwrap().unwrap()),
            None => return None,
        };
        self.next()
    }
}

impl<E, S: Stream<Item = Result<Chunk, E>> + Unpin> Stream for BufferedValueStream<S> {
    type Item = Result<FullValue, E>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(row) = self.next() {
            return Poll::Ready(Some(Ok(row)));
        }
        match futures::ready!(Pin::new(&mut self.stream).poll_next(cx)) {
            Some(Ok(chunk)) => self.chunk = chunk.peekable(),
            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
            None if self.partial_buf.len() > 0 => return Poll::Ready(Some(Ok(self.take_buf()))),
            None => return Poll::Ready(None),
        };
        self.poll_next(cx)
    }
}