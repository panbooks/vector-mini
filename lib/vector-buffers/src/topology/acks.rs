use std::{collections::VecDeque, fmt};

use num_traits::{Bounded, CheckedAdd, CheckedSub, Unsigned, WrappingAdd, WrappingSub};

#[derive(Clone, Copy, Debug, PartialEq)]
enum PendingMarkerLength<N> {
    Known(N),
    Assumed(N),
    Unknown,
}

struct PendingMarker<N, D> {
    id: N,
    len: PendingMarkerLength<N>,
    data: Option<D>,
}

impl<N, D> fmt::Debug for PendingMarker<N, D>
where
    N: fmt::Debug,
    D: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingMarker")
            .field("id", &self.id)
            .field("len", &self.len)
            .field("data", &self.data)
            .finish()
    }
}

/// The length of an eligible marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EligibleMarkerLength<N> {
    /// The marker's length was declared upfront when added.
    Known(N),

    /// The marker's length was calculated based on imperfect information, and so while it should
    /// accurately represent a correct range that covers any gaps in the marker range, it may or may
    /// not represent one true marker, or possibly multiple markers.
    Assumed(N),
}

impl<N: Copy> EligibleMarkerLength<N> {
    fn len(&self) -> N {
        match self {
            EligibleMarkerLength::Known(len) | EligibleMarkerLength::Assumed(len) => *len,
        }
    }
}

/// A marker that has been fully acknowledged.
pub struct EligibleMarker<N, D> {
    pub id: N,
    pub len: EligibleMarkerLength<N>,
    pub data: Option<D>,
}

impl<N, D> PartialEq for EligibleMarker<N, D>
where
    N: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.len == other.len
    }
}

impl<N, D> fmt::Debug for EligibleMarker<N, D>
where
    N: fmt::Debug,
    D: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EligibleMarker")
            .field("id", &self.id)
            .field("len", &self.len)
            .field("data", &self.data)
            .finish()
    }
}

/// Error returned by `OrderedAcknowledgements::add_marker`.
///
/// In general, this error represents a breaking of ID monotonicity, or more likely, the loss of
/// records where entire records may have been skipped as an attempted add provides an ID that not
/// the next expected ID.
///
/// While the exact condition must be determined by the caller, we attempt to provide as much
/// information as we reasonably based on the data we have, whether it's simply that the ID didn't
/// match the next expected ID, or that we know it is definitively ahead or behind the next expected
/// ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarkerError {
    /// The given marker ID is behind the next expected marker ID.
    ///
    /// As `OrderedAcknowledgements` expects monotonic marker IDs, this represents a violation of
    /// the acknowledgement state, and must be handled by the caller.  Generally speaking, this is
    /// an unrecoverable error.
    MonotonicityViolation,
}

/// Result of comparing a potential pending marker ID with the expected next pending marker ID.
pub enum MarkerOffset<N> {
    /// The given marker ID is aligned with the next expected marker ID.
    Aligned,

    /// The given marker ID is ahead of the next expected marker ID.
    ///
    /// When the last pending marker has a fixed-size, we can calculate the exact marker ID that we
    /// expect to see next. In turn, we can tell how far ahead the given marker ID from the next
    /// expected marker ID.
    ///
    /// The next expected marker ID, and the amount (gap) that the given marker ID and the next
    /// expected marker ID differ, are provided.
    Gap(N, N),

    /// The given marker ID may or may not be aligned.
    ///
    /// This occurs when the last pending marker has an unknown size, as we cannot determine
    /// whether the given marker ID is the next true marker ID without knowing where the last
    /// pending marker should end.
    ///
    /// The last pending marker ID is provided.
    NotEnoughInformation(N),

    /// The given marker ID is behind the next expected marker ID.
    ///
    /// As `OrderedAcknowledgements` expects monotonic marker IDs, this represents a violation of
    /// the acknowledgement state, and must be handled by the caller.  Generally speaking, this is
    /// an unrecoverable error.
    MonotonicityViolation,
}

/// `OrderedAcknowledgements` allows determining when a record is eligible for deletion.
///
/// ### Purpose
///
/// In disk buffers, a record may potentially represent multiple events. As these events
/// may be processed at different times by a sink, and in a potentially different order than when
/// stored in the record, a record cannot be considered fully processed until all of the events have
/// been accounted for.  As well, only once a record has been fully processed can it be considered
/// for deletion to free up space in the buffer.
///
/// To complicate matters, a record may sometimes not be decodable -- on-disk corruption, invalid
/// encoding scheme that is no longer supported, etc -- but still needs to be accounted for to know
/// when it can be deleted, and so that the correct metrics can be generated to determine how many
/// events were lost by the record not being able to be processed normally.
///
/// ### Functionality
///
/// `OrderedAcknowledgements` provides the ability to add "markers", which are a virtual token mapped
/// to a record. Markers track the ID of a record, how long the record is (if known), and optional
/// data that is specific to the record.  It also provides the ability to add acknowledgements which
/// can then be consumed to allow yielding markers which have collected enough acknowledgements and
/// are thus "eligible".
///
/// ### Detecting record gaps and the length of undecodable records
///
/// Additionally, and as hinted at above, markers can be added without a known length: this may
/// happen when a record is read but it cannot be decoded, and thus determining the true length is
/// not possible.
///
/// When markers that have an unknown length are added, `OrderedAcknowledgements` will do one of two things:
/// - figure out if the marker is ahead of the next expected marker ID, and add a synthetic "gap"
///   marker to compensate
/// - update the unknown length with an assumed length, based on the difference between its ID and
///   the ID of the next marker that gets added
///
/// In this way, `OrderedAcknowledgements` provides a contiguous range of marker IDs, which allows
/// detecting not only the presumed length of a record that couldn't be decoded, but also if any
/// records were deleted from disk or unable to be read at all.  Based on the invariant of expecting
/// IDs to be monotonic and contiguous, we know that if we expect our next marker ID to be 5, but
/// instead get one with an ID of 8, that there's 3 missing events in the middle that have not been
/// accounted for.
///
/// Similarly, even when we don't know what the next expected marker ID should be, we can determine
/// the number of events that were lost when the next marker is added, as marker IDs represent the
/// start of a record, and so simple arithmetic can determine the number of events that have
/// theoretically been lost.
pub struct OrderedAcknowledgements<N, D> {
    unclaimed_acks: N,
    acked_marker_id: N,
    pending_markers: VecDeque<PendingMarker<N, D>>,
}

impl<N, D> OrderedAcknowledgements<N, D>
where
    N: fmt::Display
        + Bounded
        + CheckedAdd
        + CheckedSub
        + Copy
        + PartialEq
        + PartialOrd
        + Unsigned
        + WrappingAdd
        + WrappingSub,
{
    pub fn from_acked(acked_marker_id: N) -> Self {
        Self {
            unclaimed_acks: N::min_value(),
            acked_marker_id,
            pending_markers: VecDeque::new(),
        }
    }

    /// Adds the given number of acknowledgements.
    ///
    /// Acknowledgements should be given by the caller to update the acknowledgement state before
    /// trying to get any eligible markers.
    ///
    /// # Panics
    ///
    /// Will panic if adding ack amount overflows.
    pub fn add_acknowledgements(&mut self, amount: N) {
        self.unclaimed_acks = self
            .unclaimed_acks
            .checked_add(&amount)
            .expect("overflowing unclaimed acknowledgements is a serious bug");

        trace!(
            unclaimed_acks = %self.unclaimed_acks,
            added_acks = %amount,
            "Added acknowledgements."
        );
    }

    /// Gets the marker ID offset for the given ID.
    ///
    /// If the given ID matches our next expected marker ID, then `MarkerOffset::Aligned` is
    /// returned.
    ///
    /// Otherwise, we return one of the following variants:
    /// - if we have no pending markers, `MarkerOffset::Gap` is returned, and contains the delta
    ///   between the given ID and the next expected marker ID
    /// - if we have pending markers, and the given ID is logically behind the next expected marker
    ///   ID, `MarkerOffset::MonotonicityViolation` is returned, indicating that the monotonicity
    ///   invariant has been violated
    /// - if we have pending markers, and the given ID is logically ahead of the next expected
    ///   marker, `MarkerOffset::Gap` is returned, specifying how far ahead of the next expected
    ///   marker ID it is
    /// - if we have pending markers, and the last pending marker has an unknown length,
    ///   `MarkerOffset::NotEnoughInformation` is returned, as we require a fixed-size marker to
    ///   correctly calculate the next expected marker ID
    fn get_marker_id_offset(&self, id: N) -> MarkerOffset<N> {
        if self.pending_markers.is_empty() {
            // We have no pending markers, but our acknowledged ID offset should match the marker ID
            // being given here, otherwise it would imply that the markers were not contiguous.
            //
            // We return the difference between the ID and our acknowledged ID offset with the
            // assumption that the new ID is monotonic.  Since IDs wraparound, we don't bother
            // looking at if it's higher or lower because we can't reasonably tell if this record ID
            // is actually correct but other markers in between went missing, etc.
            //
            // Basically, it's up to the caller to figure this out.  We're just trying to give them
            // as much information as we can.
            if self.acked_marker_id != id {
                return MarkerOffset::Gap(
                    self.acked_marker_id,
                    id.wrapping_sub(&self.acked_marker_id),
                );
            }
        } else {
            let back = self
                .pending_markers
                .back()
                .expect("pending markers should have items");

            // When we know the length of the previously added pending marker, we can figure out
            // where this marker's ID should land, as we don't not allow for noncontiguous marker ID
            // ranges.
            if let PendingMarkerLength::Known(len) = back.len {
                // If we know the length of the back item, then we know exactly what the ID for the
                // next marker to follow it should be.  If this incoming marker doesn't match,
                // something is wrong.
                let expected_next = back.id.wrapping_add(&len);
                if id != expected_next {
                    if expected_next < back.id && id < expected_next {
                        return MarkerOffset::MonotonicityViolation;
                    }

                    return MarkerOffset::Gap(expected_next, id.wrapping_sub(&expected_next));
                }
            } else {
                // Without a fixed-size marker, we cannot be sure whether this marker ID is aligned
                // or not.
                return MarkerOffset::NotEnoughInformation(back.id);
            }
        }

        MarkerOffset::Aligned
    }

    /// Adds a marker.
    ///
    /// The marker is tracked internally, and once the acknowledgement state has been advanced
    /// enough such that it is at or ahead of the marker, the marker will become eligible.
    ///
    /// ## Gap detection and unknown length markers
    ///
    /// When a gap is detected between the given marker ID and the next expected marker ID, we
    /// insert a synthetic marker to represent that gap.  For example, if we had a marker with an ID
    /// of 0 and a length of 5,  we would expect the next marker to have an ID of 5.  If instead, a
    /// marker with an ID of 7 was given, that would represent a gap of 2.  We insert a synthetic
    /// marker with an ID of 5 and a length of 2 before adding the marker with the ID of 7. This
    /// keeps the marker range contiguous and allows getting an eligible marker for the gap so the
    /// caller can detect that a gap occurred.
    ///
    /// Likewise, when a caller inserts an unknown length marker, we cannot know its length until
    /// the next marker is added.  When that happens, we assume the given marker ID is monotonic,
    /// and thus that the length of the previous marker, which has an unknown length, must have a
    /// length equal to the difference between the given marker ID and the unknown length marker
    /// ID.  We update the unknown length marker to reflect this.
    ///
    /// In both cases, the markers will have a length that indicates that the amount represents a
    /// gap, and not a marker that was directly added by the caller themselves.
    ///
    /// ## Errors
    ///
    /// When other pending markers are present, and the given ID is logically behind the next
    /// expected marker ID, `Err(MarkerError::MonotonicityViolation)` is returned.
    ///
    /// # Panics
    ///
    /// Panics if pending markers is empty when last pending marker is an unknown size.
    pub fn add_marker(
        &mut self,
        id: N,
        marker_len: Option<N>,
        data: Option<D>,
    ) -> Result<(), MarkerError> {
        // First, figure out where this given marker ID stands compared to our next expected marker
        // ID, and the pending marker state in general.
        match self.get_marker_id_offset(id) {
            // The last pending marker is fixed-size, and the given marker ID is past where that
            // marker ends, so we need to inject a synthetic gap marker to compensate for that.
            MarkerOffset::Gap(expected_id, amount) => {
                self.pending_markers.push_back(PendingMarker {
                    id: expected_id,
                    len: PendingMarkerLength::Assumed(amount),
                    data: None,
                });
            }
            // The last pending marker is an unknown size, so we're using this given marker ID to
            // calculate the length of that last pending marker, and in turn, we're going to adjust
            // its length before adding the new pending marker.
            MarkerOffset::NotEnoughInformation(last_marker_id) => {
                let len = id.wrapping_sub(&last_marker_id);
                let last_marker = self
                    .pending_markers
                    .back_mut()
                    .unwrap_or_else(|| unreachable!("pending markers should not be empty"));

                last_marker.len = PendingMarkerLength::Assumed(len);
            }
            // We detected a monotonicity violation, which we can't do anything about, so just
            // immediately inform the caller.
            MarkerOffset::MonotonicityViolation => return Err(MarkerError::MonotonicityViolation),
            // We have enough information to determine the given marker ID is the next expected
            // marker ID, so we can proceed normally.
            MarkerOffset::Aligned => {}
        }

        // Now insert our new pending marker.
        self.pending_markers.push_back(PendingMarker {
            id,
            len: marker_len.map_or(PendingMarkerLength::Unknown, PendingMarkerLength::Known),
            data,
        });

        Ok(())
    }

    /// Gets the next marker which has been fully acknowledged.
    ///
    /// A pending marker becomes eligible when the acknowledged marker ID is at or past the pending
    /// marker ID plus the marker length.
    ///
    /// For pending markers with an unknown length, another pending marker must be present after it
    /// in order to calculate the ID offsets and determine the marker length.
    #[cfg_attr(test, instrument(skip(self), level = "trace"))]
    pub fn get_next_eligible_marker(&mut self) -> Option<EligibleMarker<N, D>> {
        let effective_acked_marker_id = self.acked_marker_id.wrapping_add(&self.unclaimed_acks);

        trace!(
            acked_marker_id = %self.acked_marker_id,
            %effective_acked_marker_id,
            unclaimed_acks = %self.unclaimed_acks,
            pending_markers = self.pending_markers.len(),
            "Searching for eligible marker."
        );

        let maybe_eligible_marker =
            self.pending_markers
                .front()
                .and_then(|marker| match marker.len {
                    // If the acked marker ID is ahead of this marker ID, plus its length, it's been fully
                    // acknowledged and we can consume and yield the marker.  We have to double
                    // verify this by checking that there's enough unclaimed acks to support this
                    // length because otherwise we might fall victim to markers that simply generate
                    // a required acked marker ID that is equal to the actual acked marker ID when
                    // an amount of unclaimed acks exists that is not enough for this marker but is
                    // enough to align the effective/required IDs.
                    PendingMarkerLength::Known(len) => {
                        let required_acked_marker_id = marker.id.wrapping_add(&len);
                        if required_acked_marker_id <= effective_acked_marker_id
                            && self.unclaimed_acks >= len
                        {
                            Some((EligibleMarkerLength::Known(len), len))
                        } else {
                            None
                        }
                    }
                    // The marker has an assumed length, which means a marker was added after it,
                    // which implies that it is de facto eligible as unknown length markers do not
                    // consume acknowledgements and so are immediately eligible once an assumed
                    // length can be determined.
                    PendingMarkerLength::Assumed(len) => {
                        Some((EligibleMarkerLength::Assumed(len), N::min_value()))
                    }
                    // We don't yet know what the length is for this marker, so we're stuck waiting
                    // for another marker to be added before that can be determined.
                    PendingMarkerLength::Unknown => None,
                });

        // If we actually got an eligible marker, we need to actually remove it from the pending
        // marker queue and potentially adjust the amount of unclaimed acks we have.
        match maybe_eligible_marker {
            Some((len, acks_to_claim)) => {
                // If we actually got an eligible marker, we need to actually remove it from the pending
                // marker queue, potentially adjust the amount of unclaimed acks we have, and adjust
                // our acked marker ID.
                let PendingMarker { id, data, .. } = self
                    .pending_markers
                    .pop_front()
                    .unwrap_or_else(|| unreachable!("pending markers cannot be empty"));

                if acks_to_claim > N::min_value() {
                    self.unclaimed_acks = self
                        .unclaimed_acks
                        .checked_sub(&acks_to_claim)
                        .unwrap_or_else(|| {
                            unreachable!("should not be able to claim more acks than are unclaimed")
                        });
                }

                self.acked_marker_id = id.wrapping_add(&len.len());

                Some(EligibleMarker { id, len, data })
            }
            None => None,
        }
    }
}

impl<N, D> fmt::Debug for OrderedAcknowledgements<N, D>
where
    N: fmt::Debug,
    D: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedAcknowledgements")
            .field("unclaimed_acks", &self.unclaimed_acks)
            .field("acked_marker_id", &self.acked_marker_id)
            .field("pending_markers", &self.pending_markers)
            .finish()
    }
}

