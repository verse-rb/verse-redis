# Monkey patching

class BigDecimal
  def to_msgpack(*args, **opts)
    to_f.to_msgpack(*args, **opts)
  end
end