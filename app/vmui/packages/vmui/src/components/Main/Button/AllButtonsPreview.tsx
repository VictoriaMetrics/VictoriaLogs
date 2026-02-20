import Button from "./Button";
import { CopyIcon, VisibilityIcon } from "../Icons";
import ThemeControl from "../../Configurators/ThemeControl/ThemeControl";
import { useAppState } from "../../../state/common/StateContext";
import { ReactNode } from "react";

const variants = ["contained", "outlined", "text"] as const;
const colors = ["primary", "secondary", "success", "error", "gray", "warning", "white"] as const;
const sizes = ["small", "medium", "large"] as const;

const Row = ({ label, children }: { label: string; children: ReactNode }) => (
  <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8 }}>
    <div style={{ width: 80, fontWeight: "bold", textTransform: "capitalize" }}>{label}</div>
    <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>{children}</div>
  </div>
);

const Section = ({ title, color, children }: { title: string; color: string; children: ReactNode }) => (
  <div
    style={{
      padding: 48,
      background: color === "white" ? "#1E1E1E" : "transparent",
      color: color === "white" ? "#FFFFFF" : "#000000"
    }}
  >
    <h3 style={{ marginBottom: 36, textAlign: "center" }}>{title}</h3>
    <div>{children}</div>
  </div>
);

const AllButtonsPreview = () => {
  const { isDarkTheme } = useAppState();
  const bgColor = isDarkTheme ? "#1E1E1E" : "#F5F5F5";
  const color = isDarkTheme ? "#FFFFFF" : "#000000";


  return (
    <section style={{ background: bgColor, color }}>
      <div style={{ padding: 24, display: "flex", justifyContent: "center" }}>
        <ThemeControl/>
      </div>
      <div
        style={{
          padding: 24,
          fontFamily: "sans-serif",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          flexWrap: "wrap",
        }}
      >
        {variants.map((variant) => (
          <div
            key={variant}
            style={{ display: "flex", flexDirection: "column", gap: 24 }}
          >
            {colors.map((color) => (
              <Section
                key={`${variant}-${color}`}
                title={`${variant.toUpperCase()} – ${color}`}
                color={color}
              >
                {sizes.map((size) => (
                  <Row
                    key={`${variant}-${color}-${size}`}
                    label={size}
                  >
                    <Button
                      variant={variant}
                      color={color}
                      size={size}
                    >
                      {size}
                    </Button>
                    <Button
                      variant={variant}
                      color={color}
                      size={size}
                      startIcon={<CopyIcon/>}
                    >
                      {size} + icon
                    </Button>
                    <Button
                      variant={variant}
                      color={color}
                      size={size}
                      startIcon={<CopyIcon/>}
                    />
                  </Row>
                ))}

                <Row label="disabled">
                  <Button
                    variant={variant}
                    color={color}
                    disabled
                  >
                    Disabled
                  </Button>
                </Row>

                <Row label="start">
                  <Button
                    variant={variant}
                    color={color}
                    startIcon={<CopyIcon/>}
                  >
                    Start Icon
                  </Button>
                </Row>

                <Row label="end">
                  <Button
                    variant={variant}
                    color={color}
                    endIcon={<VisibilityIcon/>}
                  >
                    End Icon
                  </Button>
                </Row>

                <Row label="both">
                  <Button
                    variant={variant}
                    color={color}
                    startIcon={<VisibilityIcon/>}
                    endIcon={<CopyIcon/>}
                  >
                    Icons Both
                  </Button>
                </Row>

                <Row label="icon">
                  <Button
                    variant={variant}
                    color={color}
                    startIcon={<CopyIcon/>}
                  />
                  <Button
                    variant={variant}
                    color={color}
                    startIcon={<VisibilityIcon/>}
                  />
                </Row>
              </Section>
            ))}
          </div>
        ))}
      </div>
    </section>
  );
};

export default AllButtonsPreview;
