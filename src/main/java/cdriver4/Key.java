package cdriver4;

public class Key {
  private final String name;
  private final String value;

  public Key(String name, String value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }
}
