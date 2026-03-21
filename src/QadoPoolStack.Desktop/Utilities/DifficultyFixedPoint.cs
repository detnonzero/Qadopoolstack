using System.Globalization;
using System.Numerics;

namespace QadoPoolStack.Desktop.Utilities;

public static class DifficultyFixedPoint
{
    // Six decimal places keep per-share weights precise enough for payout fairness
    // while staying well within 64-bit storage for the pool's current difficulty range.
    public const long Scale = 1_000_000L;
    private static readonly decimal ScaleDecimal = Scale;

    public static long ToScaled(double difficulty)
    {
        var normalized = NormalizeDecimal(difficulty);
        var scaled = decimal.Round(normalized * ScaleDecimal, 0, MidpointRounding.AwayFromZero);
        if (scaled < ScaleDecimal)
        {
            scaled = ScaleDecimal;
        }

        if (scaled > long.MaxValue)
        {
            throw new InvalidOperationException("Share difficulty exceeds the fixed-point storage range.");
        }

        return (long)scaled;
    }

    public static double ToNormalizedDouble(double difficulty)
        => ToScaled(difficulty) / (double)Scale;

    private static decimal NormalizeDecimal(double difficulty)
    {
        if (double.IsNaN(difficulty) || double.IsInfinity(difficulty) || difficulty <= 0d)
        {
            return 1m;
        }

        var text = difficulty.ToString("G17", CultureInfo.InvariantCulture);
        if (!decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0m)
        {
            return 1m;
        }

        return parsed < 1m ? 1m : parsed;
    }
}

public static class LargestRemainderAllocation
{
    public static List<(string participantId, long amountAtomic)> Allocate(
        long totalAtomic,
        IEnumerable<(string participantId, BigInteger weight)> weightedParticipants)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(totalAtomic);

        var participants = weightedParticipants
            .Where(item => !string.IsNullOrWhiteSpace(item.participantId) && item.weight > BigInteger.Zero)
            .OrderBy(item => item.participantId, StringComparer.Ordinal)
            .ToList();

        if (totalAtomic == 0 || participants.Count == 0)
        {
            return [];
        }

        var totalWeight = participants.Aggregate(BigInteger.Zero, (current, item) => current + item.weight);
        if (totalWeight <= 0)
        {
            throw new InvalidOperationException("Largest remainder allocation requires a positive total weight.");
        }

        var allocations = new List<AllocationState>(participants.Count);
        long distributedAtomic = 0;
        var totalAtomicBigInteger = new BigInteger(totalAtomic);

        foreach (var participant in participants)
        {
            var numerator = totalAtomicBigInteger * participant.weight;
            var baseAmount = BigInteger.DivRem(numerator, totalWeight, out var remainder);
            var baseAmountAtomic = checked((long)baseAmount);
            distributedAtomic = checked(distributedAtomic + baseAmountAtomic);
            allocations.Add(new AllocationState(participant.participantId, baseAmountAtomic, remainder));
        }

        var remainingAtomic = totalAtomic - distributedAtomic;
        if (remainingAtomic < 0)
        {
            throw new InvalidOperationException("Largest remainder allocation over-distributed atomic units.");
        }

        if (remainingAtomic > allocations.Count)
        {
            throw new InvalidOperationException("Largest remainder allocation produced more leftover units than participants.");
        }

        foreach (var allocation in allocations
                     .OrderByDescending(item => item.Remainder)
                     .ThenBy(item => item.ParticipantId, StringComparer.Ordinal)
                     .Take((int)remainingAtomic))
        {
            allocation.AmountAtomic++;
        }

        return allocations
            .Where(item => item.AmountAtomic > 0)
            .OrderBy(item => item.ParticipantId, StringComparer.Ordinal)
            .Select(item => (item.ParticipantId, item.AmountAtomic))
            .ToList();
    }

    private sealed class AllocationState
    {
        public AllocationState(string participantId, long amountAtomic, BigInteger remainder)
        {
            ParticipantId = participantId;
            AmountAtomic = amountAtomic;
            Remainder = remainder;
        }

        public string ParticipantId { get; }

        public long AmountAtomic { get; set; }

        public BigInteger Remainder { get; }
    }
}
