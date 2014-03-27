package org.opencb.commons.bioformats.alignment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import javafx.util.converter.ByteStringConverter;
import net.sf.samtools.Cigar;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;

/**
 * Information about a sequence alignment.
 * 
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class Alignment {
    
    private String name;
    
    private String chromosome;
    private long start;
    private long end;
    private long unclippedStart;
    private long unclippedEnd;
    
    private int length;
    private int mappingQuality;
    private String qualities;   // TODO Find an alternative way to store qualities
    private String mateReferenceName;
    private int mateAlignmentStart;
    private int inferredInsertSize;
    private byte[] readSequence;    // can be null

    /**
     * List of differences between the reference sequence and this alignment. 
     * Each one is defined by its position, type of difference and the changes it
     * introduces.
     */
    private List<AlignmentDifference> differences;
    
    /**
     * Optional attributes that probably depend on the format of the file the 
     * alignment was initially read.
     */
    private Map<String, String> attributes;
    
    /**
     * Bitmask with information about structure, quality and other properties 
     * of the alignment.
     */
    private int flags;
    
    public static final int ALIGNMENT_MULTIPLE_SEGMENTS = 0x01;
    public static final int SEGMENTS_PROPERLY_ALIGNED = 0x02;
    public static final int SEGMENT_UNMAPPED = 0x04;
    public static final int NEXT_SEGMENT_UNMAPPED = 0x08;
    public static final int SEQUENCE_REVERSE_COMPLEMENTED = 0x10;
    public static final int SEQUENCE_NEXT_SEGMENT_REVERSED = 0x20;
    public static final int FIRST_SEGMENT = 0x40;
    public static final int LAST_SEGMENT = 0x80;
    public static final int SECONDARY_ALIGNMENT = 0x100;
    public static final int NOT_PASSING_QC = 0x200;
    public static final int PCR_OR_OPTICAL_DUPLICATE = 0x400;
    public static final int SUPPLEMENTARY_ALIGNMENT = 0x800;

    public Alignment() { }
    
    public Alignment(String name, String chromosome, long start, long end, long unclippedStart, long unclippedEnd, 
            int length, int mappingQuality, String qualities, String mateReferenceName, int mateAlignmentStart, 
            int inferredInsertSize, int flags, List<AlignmentDifference> differences, Map<String, String> attributes) {
        this.name = name;
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.unclippedStart = unclippedStart;
        this.unclippedEnd = unclippedEnd;
        this.length = length;
        this.mappingQuality = mappingQuality;
        this.qualities = qualities;
        this.mateReferenceName = mateReferenceName;
        this.mateAlignmentStart = mateAlignmentStart;
        this.inferredInsertSize = inferredInsertSize;
        this.flags = flags;
        this.differences = differences;
        this.attributes = attributes;
    }

    public Alignment(SAMRecord record, Map<String, String> attributes, String referenceSequence) {
        this(record.getReadName(), record.getReferenceName(), record.getAlignmentStart(), record.getAlignmentEnd(), 
                record.getUnclippedStart(), record.getUnclippedEnd(), record.getReadLength(), 
                record.getMappingQuality(), record.getBaseQualityString(),//.replace("\\", "\\\\").replace("\"", "\\\""), 
                record.getMateReferenceName(), record.getMateAlignmentStart(), 
                record.getInferredInsertSize(), record.getFlags(), 
                AlignmentHelper.getDifferencesFromCigar(record, referenceSequence, Integer.MAX_VALUE),
                attributes);
        readSequence = record.getReadBases();
    }
    public Alignment(SAMRecord record, String referenceSequence) {
        this(record.getReadName(), record.getReferenceName(), record.getAlignmentStart(), record.getAlignmentEnd(),
                record.getUnclippedStart(), record.getUnclippedEnd(), record.getReadLength(),
                record.getMappingQuality(), record.getBaseQualityString(),//.replace("\\", "\\\\").replace("\"", "\\\""),
                record.getMateReferenceName(), record.getMateAlignmentStart(),
                record.getInferredInsertSize(), record.getFlags(),
                AlignmentHelper.getDifferencesFromCigar(record, referenceSequence, Integer.MAX_VALUE),
                null);
        readSequence = record.getReadBases();
        attributes = new HashMap<>();
        for(SAMRecord.SAMTagAndValue tav : record.getAttributes()){
            attributes.put(tav.tag, tav.value.toString());
        }
    }

    SAMRecord createSAMRecord(SAMFileHeader samFileHeader, String referenceSequence){
        return createSAMRecord(samFileHeader, referenceSequence, start);
    }
    SAMRecord createSAMRecord(SAMFileHeader samFileHeader, String referenceSequence, long referenceSequenceStartPosition){
        SAMRecord samRecord = new SAMRecord(samFileHeader);

        samRecord.setReadName(name);
        samRecord.setReferenceName(chromosome);
        samRecord.setAlignmentStart((int)start);
        //samRecord.setAlignmentEnd((int)end);

        samRecord.setMappingQuality(mappingQuality);
        samRecord.setBaseQualities(qualities.getBytes());

        samRecord.setMateReferenceName(mateReferenceName);
        samRecord.setMateAlignmentStart(mateAlignmentStart);

        samRecord.setInferredInsertSize(inferredInsertSize);
        samRecord.setFlags(flags);



        samRecord.setCigar(AlignmentHelper.getCigarFromDifferences(differences, length));
        readSequence = AlignmentHelper.getSequenceFromDifferences(differences, length, referenceSequence, (int)(start-referenceSequenceStartPosition)).getBytes();


        //if(readSequence == null){
            //getSequenceFromDifferences();
        //}
        samRecord.setReadBases(readSequence);

        samRecord.setBaseQualities(qualities.getBytes());


        return samRecord;
    }
    
    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public List<AlignmentDifference> getDifferences() {
        return differences;
    }

    public void setDifferences(List<AlignmentDifference> differences) {
        this.differences = differences;
    }

    public boolean addDifference(AlignmentDifference d) {
        return differences.add(d);
    }
    
    public boolean removeDifference(int position) {
        for (AlignmentDifference d : differences) {
            if (d.getPos() == position) {
                return differences.remove(d);
            }
        }
        return false;
    }

    public boolean completeDifferences(String referenceSequence){
        return completeDifferences(referenceSequence, 0);
    }
    public boolean completeDifferences(String referenceSequence, int offset){
        for(AlignmentDifference alignmentDifference : differences){
            if(alignmentDifference.getSeq() == null || !alignmentDifference.isAllSequenceStored()){
                try{
                    alignmentDifference.setSeq(
                            referenceSequence.substring(
                                    alignmentDifference.getPos() + offset,
                                    alignmentDifference.getPos() + offset + alignmentDifference.getLength()
                            )
                    );
                } catch (StringIndexOutOfBoundsException e){
                    System.out.println("referenceSequence Out of Bounds in \"Alignment.completeDifferences()\"" + e.toString());
                    return false;
                }
            }
        }

        return true;
    }
    
    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }
    
    public void addFlag(int flag) {
        this.flags |= flag;
    }
    
    public void removeFlag(int flag) {
        this.flags = this.flags & ~flag;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public boolean addAttribute(String key, String value) {
        return attributes.put(key, value) != null;
    }
    
    public String removeAttribute(String key) {
        return attributes.remove(key);
    }
    
    public int getInferredInsertSize() {
        return inferredInsertSize;
    }

    public void setInferredInsertSize(int inferredInsertSize) {
        this.inferredInsertSize = inferredInsertSize;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getMappingQuality() {
        return mappingQuality;
    }

    public void setMappingQuality(int mappingQuality) {
        this.mappingQuality = mappingQuality;
    }

    public int getMateAlignmentStart() {
        return mateAlignmentStart;
    }

    public void setMateAlignmentStart(int mateAlignmentStart) {
        this.mateAlignmentStart = mateAlignmentStart;
    }

    public String getMateReferenceName() {
        return mateReferenceName;
    }

    public void setMateReferenceName(String mateReferenceName) {
        this.mateReferenceName = mateReferenceName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQualities() {
        return qualities;
    }

    public void setQualities(String qualities) {
        this.qualities = qualities;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getUnclippedEnd() {
        return unclippedEnd;
    }

    public void setUnclippedEnd(long unclippedEnd) {
        this.unclippedEnd = unclippedEnd;
    }

    public long getUnclippedStart() {
        return unclippedStart;
    }

    public void setUnclippedStart(long unclippedStart) {
        this.unclippedStart = unclippedStart;
    }

    public byte[] getReadSequence() {
        return readSequence;
    }

    public void setReadSequence(byte[] readSequence) {
        this.readSequence = readSequence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;


        Alignment alignment = (Alignment) o;

        if (end != alignment.end) return false;
        if (flags != alignment.flags) return false;
        if (inferredInsertSize != alignment.inferredInsertSize) return false;
        if (length != alignment.length) return false;
        if (mappingQuality != alignment.mappingQuality) return false;
        if (mateAlignmentStart != alignment.mateAlignmentStart) return false;
        if (start != alignment.start) return false;
        if (unclippedEnd != alignment.unclippedEnd) return false;
        if (unclippedStart != alignment.unclippedStart) return false;
        if (!attributes.equals(alignment.attributes)) return false;
        if (!chromosome.equals(alignment.chromosome)) return false;
        if (!differences.equals(alignment.differences)) return false;
        if (!mateReferenceName.equals(alignment.mateReferenceName)) return false;
        if (!name.equals(alignment.name)) return false;
        if (!qualities.equals(alignment.qualities)) return false;
        if (!Arrays.equals(readSequence, alignment.readSequence)) return false;
//
//        if (readSequence == null ^ alignment.readSequence == null) { // only one is null
//            return false;
//        } else if (readSequence != null && !Arrays.equals(readSequence, alignment.readSequence)) {  // both are not null and different
//            return false;
//        }

        return true;
    }

    public static class AlignmentDifference {
        
        private final int pos;  // in the reference sequence
        private final char op;
        private String seq;   // seq might not store the complete sequence: seq.length() will be shorter
        private final int length;   // this length is the real length of the sequence

        public static final char INSERTION = 'I';
        public static final char DELETION = 'D';
        public static final char MISMATCH = 'X';
        public static final char SKIPPED_REGION = 'N';
        public static final char SOFT_CLIPPING = 'S';
        public static final char HARD_CLIPPING = 'H';
        public static final char PADDING = 'P';

        public AlignmentDifference(int pos, char op, String seq, int length) {
            this.pos = pos;
            this.op = op;
            this.seq = seq;
            this.length = length;
        }

        public AlignmentDifference(int pos, char op, String seq) {
            this(pos, op, seq, seq.length());
        }

        public AlignmentDifference(int pos, char op, int length) {
            this(pos, op, null, length);
        }

        public char getOp() {
            return op;
        }

        public int getPos() {
            return pos;
        }

        public String getSeq() {
            return seq;
        }
        public void setSeq(String seq) {
            this.seq = seq;
        }

        public int getLength() {
            return this.length;
        }

        public boolean isAllSequenceStored() {  //Maybe is only stored a partial sequence
            return seq != null && seq.length() == length;
        }
        public boolean isSequenceStored() {
            return seq != null;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof AlignmentDifference)) { return false; }
            
            AlignmentDifference other = (AlignmentDifference) obj;
            if (isSequenceStored()) {
            return pos == other.pos && op == other.op && seq.equalsIgnoreCase(other.seq) && length == other.length;
            } else {
                return pos == other.pos && op == other.op && length == other.length;
            }
        }

        @Override
        public String toString() {
            return String.format("%d: %d %c %s", pos, length, op, seq);
        }

    }
    
}
