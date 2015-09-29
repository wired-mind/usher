package io.cozmic.usher.test;

import java.io.Serializable;

public class Pojo implements Serializable {
	String favoriteColor;
	String name;
	String phone;
	Integer favoriteNumber;
	
	public Pojo(String name, String phone, int favoriteNumber, String favoriteColor) {
		this.favoriteColor = favoriteColor;
		this.name = name;
		this.phone = phone;
		this.favoriteNumber = favoriteNumber;
	};
	
	public String getFavoriteColor() {
		return favoriteColor;
	}

	public Pojo() {}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public int getFavoriteNumber() {
		return favoriteNumber;
	}

	public void setFavoriteNumber(int favoriteNumber) {
		this.favoriteNumber = favoriteNumber;
	}

	public void setFavoriteColor(String favoriteColor) {
		this.favoriteColor = favoriteColor;
	};

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Pojo pojo = (Pojo) o;

		if (favoriteColor != null ? !favoriteColor.equals(pojo.favoriteColor) : pojo.favoriteColor != null)
			return false;
		if (favoriteNumber != null ? !favoriteNumber.equals(pojo.favoriteNumber) : pojo.favoriteNumber != null)
			return false;
		if (name != null ? !name.equals(pojo.name) : pojo.name != null) return false;
		if (phone != null ? !phone.equals(pojo.phone) : pojo.phone != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = favoriteColor != null ? favoriteColor.hashCode() : 0;
		result = 31 * result + (name != null ? name.hashCode() : 0);
		result = 31 * result + (phone != null ? phone.hashCode() : 0);
		result = 31 * result + (favoriteNumber != null ? favoriteNumber.hashCode() : 0);
		return result;
	}
}
