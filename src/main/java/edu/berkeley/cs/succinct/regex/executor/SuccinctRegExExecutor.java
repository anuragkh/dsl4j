package edu.berkeley.cs.succinct.regex.executor;

import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.SuccinctRegExMatch;
import edu.berkeley.cs.succinct.regex.parser.RegEx;
import edu.berkeley.cs.succinct.regex.parser.RegExWildcard;

import java.util.TreeSet;

public abstract class SuccinctRegExExecutor extends RegExExecutor {

  /**
   * Constructor to initialize SuccinctRegExExecutor.
   *
   * @param succinctFile The input SuccinctFile.
   * @param regEx        The input regular expression.
   */
  SuccinctRegExExecutor(SuccinctFile succinctFile, RegEx regEx) {
    super(succinctFile, regEx);
  }

  /**
   * Converts Succinct regex matches (i.e., SA ranges) to actual regex matches.
   * @param rangeRes Results as a set of ranges.
   * @param sortType Sort Type for output.
   * @return Results as actual regex matches.
   */
  protected TreeSet<RegExMatch> rangeResultsToRegexMatches(TreeSet<SuccinctRegExMatch> rangeRes, SortType sortType) {
    TreeSet<RegExMatch> regExMatches = allocateSet(sortType);
    for (SuccinctRegExMatch match : rangeRes) {
      if (!match.empty()) {
        Long[] offsets = succinctFile.rangeToOffsets(match);
        for (long offset : offsets) {
          regExMatches.add(new RegExMatch(offset, match.getLength()));
        }
      }
    }
    return regExMatches;
  }

  /**
   * Computes the regular expression with forward search.
   *
   * @param r The regular expression.
   * @return The results for the regular expression.
   */
  @Override protected TreeSet<RegExMatch> compute(RegEx r, SortType sortType) {
    TreeSet<RegExMatch> results;
    switch (r.getRegExType()) {
      case Wildcard: {
        RegExWildcard w = (RegExWildcard) r;
        TreeSet<RegExMatch> leftResults = compute(w.getLeft(), SortType.END_SORTED);
        TreeSet<RegExMatch> rightResults = compute(w.getRight(), SortType.FRONT_SORTED);
        results = regexWildcard(leftResults, rightResults, sortType);
        break;
      }
      default: {
        TreeSet<SuccinctRegExMatch> succinctResults = computeSuccinctly(r);
        results = rangeResultsToRegexMatches(succinctResults, sortType);
      }
    }
    return results;
  }

  /**
   * Uses Succinct data representation to compute regular expression.
   *
   * @param r Regular expression to compute.
   * @return The results of the regular expression.
   */
  protected abstract TreeSet<SuccinctRegExMatch> computeSuccinctly(RegEx r);
}
