package search;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * @Author: JMD
 * @Date: 3/14/2023
 */

@Getter @Setter
public class Poetry {
    String id;
    String author;
    String title;
    String verse;
    String section;
    String rhythmic;
    List<String> tags;

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    List<String> content;
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    List<String> paragraphs;
    String note;

    boolean makeVerse(){
        if(paragraphs != null){
            verse = String.join("\n", paragraphs);
        }
        if(content != null){
            verse = String.join("\n", content);
        }
        return !verse.isEmpty();
    }

    @Override
    public String toString() {
        makeVerse();
        return "Poetry{" +
                "author='" + author + '\'' +
                ", title='" + title + '\'' +
                ", text='" + verse + '\'' +
                ", section='" + section + '\'' +
                ", rhythmic='" + rhythmic + '\'' +
                ", tags=" + tags +
                ", note='" + note + '\'' +
                '}';
    }
}
